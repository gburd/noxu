# Noxu DB Re-Audit — Keith Bostic

## Performance and Correctness: Lingering and New Issues

**Date**: 2026-05-30  
**Reviewer**: Keith Bostic (re-review persona)  
**Branch examined**: `origin/main` at commit 8f63f6e  
**Worktree**: `/tmp/reaudit-keith`  
**Methodology**: READ-ONLY static analysis of post-wave-11-U codebase against
prior audit docs (`audit-2026-05-keith.md`, `audit-2026-05-synthesis.md`,
`wave-11-{q,s,u,j,k}.md`). Prior findings C-1..C-9, H-1..H-10, X-1..X-16
are NOT re-reported unless the fix is incomplete or a new dimension was found.

---

## Prior Fix Status (informational, don't re-report)

The following were fixed in waves 11-Q/R/S/T/U and are confirmed closed in
`origin/main`:

| Finding | Wave | Status |
|---|---|---|
| C-1 parent-dir fsync | Q | ✓ Fixed |
| C-2 fsync invalidates env (io_invalid AtomicBool) | Q | ✓ Fixed |
| C-3 CRC in recovery scanner | Q | ✓ Fixed |
| C-7 Release/Acquire on pin-count | Q | ✓ Fixed |
| H-1 env lock held across abort undo | S | ✓ Fixed |
| H-2 lock ordering waiter_graph+shard | Q | ✓ Fixed |
| H-4 deadlock victim selection | Q | ✓ Fixed |
| H-9 PartialEvict actually frees data | Q | ✓ Fixed |
| H-3 per-log-entry alloc (LWL scratch buffer) | S | ✓ Partial (see R-1) |
| C-4 open_database txn semantics | R | ✓ Fixed |
| C-5 BIN delta guard clauses | R | ✓ Fixed |
| C-6 MapLN two-pass recovery | U | ⚠ Partial (see R-5) |
| X-2 VLSN persistence capped at checkpoint | U | ✓ Fixed |
| X-4 resolving_xids sentinel | U | ✓ Fixed |
| X-5 cleaner checkpoint barrier | U | ✓ Fixed |
| X-6 migration WAL entry | U | ✓ Fixed (minor residual, R-7) |
| X-7 secondary LN cleaner dispatch | U | ✓ Fixed |
| X-8 redundant empty BINDelta | U | ✓ Fixed |
| X-11 LogFlushTask daemon | U | ✓ Implemented (new perf issue, P-1) |
| X-12 cache_size budget split | U | ✓ Fixed (minor residual, R-6) |
| X-13 io_invalid propagates to reads/cursors | U | ✓ Fixed |
| X-1 VLSN index truncated after rollback | U | ✓ Fixed |
| X-3 recovered XA commit assigns real VLSN | U | ✓ Fixed (residual, R-3) |
| X-14 VLSN index rebuilt during recovery | U | ✓ Fixed |
| X-15 open-ended rollback period | U | ✓ Fixed |

---

## New and Lingering Findings

---

### R-1 — H-3 partial: `collect_dirty_buffers` still allocates `Vec<(Vec<u8>, u64)>` per flush [LINGERING PERF — HIGH]

**Severity**: High  
**Subsystem**: noxu-log write path  
**File:line**: `crates/noxu-log/src/log_manager.rs:594–629`  
**Prior reference**: Keith F-1.2, synthesis H-3

Wave 11-S fixed the per-entry `vec![0u8; entry_size]` allocation by embedding a
scratch `Vec<u8>` inside the `log_write_latch: Mutex<Vec<u8>>`. That eliminates
F-1.1 / H-3's first allocation. The **second allocation** cited in F-1.2 —
`Vec<(Vec<u8>, u64)>` in `collect_dirty_buffers` — is still present:

```rust
// log_manager.rs:594–629
fn collect_dirty_buffers(&self) -> Vec<(Vec<u8>, u64)> {
    let mut pending: Vec<(Vec<u8>, u64)> = Vec::new();
    // …
    let data = unflushed.to_vec();   // ← allocation per dirty buffer
    pending.push((data, offset));
```

This allocates a fresh `Vec<u8>` for every dirty buffer segment on every
`flush_sync`, `flush_no_sync`, and `flush_sync_if_needed` call — i.e., on
every transactional commit that triggers a flush.

**Wave 11-S benchmark result** (`W01`/`W06`): +1.9% / −4.1% — within noise,
consistent with the per-buffer allocation still dominating. The wave docs
acknowledge: "No regression observed; change retained" but the collect path was
not addressed.

**Failure mode**: Allocator pressure on every commit path; at 10x concurrent
writers the `malloc`/`free` contention from this path is the dominant per-commit
overhead. Wave 11-H profile showed `malloc` at 11.85% in W11 — a significant
portion of this is this path.

**Suggested fix**: Replace `collect_dirty_buffers` return type with a reused
`Vec<(Bytes, u64)>` where `Bytes` is a slice view into the already-allocated log
buffer memory (zero copy), or pass a pre-allocated `Vec` from the LWL scratch
struct into `collect_dirty_buffers` and reuse it across calls. Since the LWL is
held for the entire collect + write sequence, a single per-`LogManager` buffer
list is safe.

---

### R-2 — `LogFlushTask` background daemon holds LWL across `pwrite64` — blocks all foreground commits [NEW — HIGH]

**Severity**: High  
**Subsystem**: noxu-dbi environment, noxu-log log manager  
**File:line**: `crates/noxu-log/src/log_manager.rs:568–581` (`flush_no_sync`);
`crates/noxu-dbi/src/environment_impl.rs:822–824` (LogFlushTask caller)  
**Prior reference**: X-11 (new in cross-feature audit; wave-11-U implemented it)

The LogFlushTask daemon (X-11 fix) calls `lm.flush_no_sync()` on a background
thread at the configured interval. `flush_no_sync` acquires the LWL
(`log_write_latch`) and holds it across ALL `write_buffer` / `pwrite64` kernel
calls:

```rust
// log_manager.rs:568–581
pub fn flush_no_sync(&self) -> Result<Lsn> {
    let eol = {
        let _lwl = self.log_write_latch.lock();   // ← LWL held
        let pending = self.collect_dirty_buffers();
        let eol = self.file_manager.get_next_available_lsn();
        for (data, offset) in pending {           // ← pwrite64 here
            self.file_manager.write_buffer(&data, offset)?;
        }
        eol
    };                                             // ← LWL released
```

The LWL (`log_write_latch`) is the same mutex that every foreground
`log_internal` call must acquire to write any log entry. While the background
`flush_no_sync` holds the LWL and is blocked on `pwrite64`, ALL concurrent
transaction commits — whether `NoSync`, `WriteNoSync`, or `Sync` — stall at
`log_write_latch.lock()`.

On a disk that takes 2–5 ms per `pwrite64` (typical NVMe under load), a
background flush every N milliseconds injects a 2–5 ms stall burst into ALL
foreground writers. With 8 concurrent writers, each experiencing the stall in
sequence, the effective per-commit tail latency becomes dominated by the
background flush.

The intended purpose of `LogFlushTask` is to bound the latency of
`CommitNoSync` commits: data should reach the OS page cache within the
configured interval even if no subsequent commit triggers a flush. The current
implementation achieves this goal but at the cost of serialising ALL foreground
commits whenever the background flush fires.

**Failure mode**: At production scale with `log_flush_no_sync_interval_ms > 0`
(non-zero — the feature must be explicitly enabled), every N ms a stall spike of
up to `N_dirty_buffers × pwrite64_latency` is imposed on all writers. Under
heavy load this manifests as regular commit latency spikes, not visible in
average-throughput benchmarks but highly visible in tail-latency p99/p999
measurements.

**Suggested fix**: The background flush should release the LWL between
individual `write_buffer` calls, or better, use a separate `flush_no_sync`
implementation that does NOT hold the LWL during I/O. The standard approach is:
(1) snapshot buffer references under the LWL, (2) release LWL, (3) issue
pwrite64 calls without the LWL, (4) reacquire LWL briefly to advance the
`last_flush_lsn` watermark. The foreground `flush_sync` path deliberately holds
the LWL through pwrite64 to coalesce concurrent writers at the FsyncManager; the
background flush has no such coalescing requirement and should not hold the LWL
during I/O.

---

### R-3 — Recovered XA commit VLSN lost after second crash [NEW — MEDIUM]

**Severity**: Medium  
**Subsystem**: noxu-db XA, noxu-recovery  
**File:line**: `crates/noxu-db/src/environment.rs:1503–1513` (`write_txn_end_for_recovered`);
`crates/noxu-rep/src/replicated_environment.rs:2102–2120` (`alloc_vlsn_for_recovered_commit`)  
**Prior reference**: X-3 (fixed in wave-11-U)

The X-3 fix correctly calls `alloc_vlsn_for_recovered_commit` after writing the
TxnCommit WAL entry for a recovered XA transaction. This registers the VLSN in
the in-memory `VlsnIndex`. However, the TxnCommit WAL entry itself is still
written with `NULL_VLSN`:

```rust
// environment.rs:1503–1513
let entry = if is_commit {
    TxnEndEntry::new_commit(
        txn_id as i64,
        NULL_LSN,
        timestamp,
        0,
        NULL_VLSN,   // ← still NULL_VLSN in the WAL entry
    )
```

The X-14 fix rebuilds the VLSN index from recovery by scanning LN records that
carry a non-zero VLSN field. TxnCommit records are NOT scanned for VLSNs
(`recovery_manager.rs:671–690`). Therefore:

1. XA transaction committed on master → VLSN registered in `VlsnIndex` (in-memory).  
2. Master crashes again before next `vlsn.idx` flush.  
3. Recovery replays WAL: sees TxnCommit with NULL_VLSN → X-14 rebuild skips it.  
4. Post-recovery `VlsnIndex` does NOT include the recovered XA commit's VLSN.  
5. Feeders claim this VLSN was never committed; replicas may never receive it.

**Failure mode**: Rare (requires crash after XA resolution AND before vlsn.idx
flush), but when it occurs the recovered XA commit's data is present in the
B-tree but invisible to the replication layer after the second recovery. Replicas
may not converge to include those writes without a full network restore.

**Suggested fix**: Write the TxnCommit WAL entry for recovered XA commits with a
real allocated VLSN embedded in the header (same as the normal `Transaction::commit`
replication path). Alternatively, scan TxnCommit entries during X-14 VLSN
rebuild if they carry a non-NULL VLSN. The X-14 code already has the plumbing;
it just doesn't handle TxnCommit entries.

---

### R-4 — `commit_pending_database` TOCTOU window: database visible in neither `name_map` nor `pending_names` [NEW — MEDIUM]

**Severity**: Medium  
**Subsystem**: noxu-dbi environment  
**File:line**: `crates/noxu-dbi/src/environment_impl.rs:1122–1144` (`commit_pending_database`)  
**Prior reference**: C-4 (fixed in wave-11-R), C-6 (partial in wave-11-U)

`commit_pending_database` has two separate lock acquisitions with a gap between them:

```rust
// environment_impl.rs:1122–1144
pub fn commit_pending_database(&self, name: &str) {
    let db_id = {
        let mut pending = self.pending_names.write();
        pending.remove(name);               // ← (1) removed from pending_names
        // pending_names write lock dropped here via closing brace
        self.db_map.read().iter().find_map(…)
    };
    if let Some(db_id) = db_id {
        self.name_map.write().insert(…);    // ← (2) inserted into name_map
    }
}
```

Between step (1) (name removed from `pending_names`) and step (2) (name inserted
into `name_map`), the database exists in `db_map` but in NEITHER `pending_names`
NOR `name_map`. A concurrent `open_database` call for the same name at this
moment will:

1. Check `name_map.read()` → not found.  
2. Check `pending_names` → not found.  
3. If `allow_create=true` → attempt to create a NEW database with the same name
   and a freshly allocated `db_id`.  
4. This second `db_id` will be registered in `db_map`, displacing the original.

The original `db_id`'s entry then completes step (2) and inserts itself into
`name_map`. Now `name_map` has the original `db_id` but `db_map` has been
overwritten with the new one. The reference count on the original `DatabaseImpl`
is orphaned.

**Failure mode**: Two concurrent threads — one committing a transactional
`open_database`, one simultaneously calling `open_database` for the same name
with `allow_create` — can create a phantom database duplication with a
use-after-free risk on the `DatabaseImpl`'s reference count. Rare in practice
(requires precise interleaving), but the code has no guard against it.

**Suggested fix**: Hold `pending_names` write lock across the entire
`commit_pending_database` operation, including the `name_map.write().insert()`.
Or, use a single `RwLock<(HashMap<String, DatabaseId>, HashSet<String>)>` that
covers both maps atomically.

Additionally, `commit_pending_database` performs an **O(N) linear scan** over
`db_map` (`iter().find_map(...)`) to locate the `db_id` by name. At 10x scale
(large embedded environments with hundreds of open databases), this is
measurably slow and runs while holding `pending_names.write()`. Store the
`db_id` in `pending_names` as a `HashMap<String, DatabaseId>` rather than a
`HashSet<String>` to allow O(1) lookup.

---

### R-5 — C-6 partial fix gap: `NameLNTxn` is still written at commit time for the non-transactional path [LINGERING — MEDIUM]

**Severity**: Medium  
**Subsystem**: noxu-dbi, noxu-recovery  
**File:line**: `crates/noxu-dbi/src/environment_impl.rs:963–981` (`open_database`);
`crates/noxu-recovery/src/recovery_manager.rs:246–260` (TODO comment)  
**Prior reference**: C-6 (partial fix wave-11-R/U)

The wave-11-U doc acknowledges: "Noxu currently writes the NameLN WAL entry at
commit time (not inside the transaction). So `recovered_db_txn_ids` is always
empty for current WAL files — there are no NameLN entries with txn_ids to undo."

The C-6 partial fix correctly handles the TRANSACTIONAL path via
`open_database_transactional`, which writes `NameLNTxn` with `Provisional::Yes`
inside the creating transaction. However, the default `open_database()` path
(called when `txn=None`) still inserts `name` into `name_map` immediately and
writes a plain `NameLN` at that point — not inside any transaction.

Furthermore, the recovery `run_mapping_tree_undo_pass` depends on
`recovered_db_txn_ids` being populated, which only happens for `NameLNTxn`
entries. For the legacy non-transactional path, `recovered_db_txn_ids` is empty,
meaning the undo pass is effectively a no-op for all currently-created databases.

**Failure mode**: Crash during a transactional database creation where the user
passed `txn=None` (or used the higher-level `Environment::open_database(None,
...)` API) leaves the database registered in the WAL as if committed, even if the
surrounding application logic wanted to roll it back. Recovery will restore the
database name as committed. This is the C-6 bug that was only partially fixed.

**Suggested fix**: Complete the C-6 fix: route ALL `open_database` calls through
`open_database_transactional` when called within an auto-commit context, and
ensure the plain non-transactional path (truly no-txn) still uses `NameLN`
directly. The two cases need to be distinguished at the API level.

---

### R-6 — X-12 residual: `arbiter_budget` clamp does not prevent total memory exceeding `cache_size` [LINGERING — LOW]

**Severity**: Low  
**Subsystem**: noxu-dbi environment, memory budget  
**File:line**: `crates/noxu-dbi/src/environment_impl.rs:557–575`  
**Prior reference**: X-12 (fixed in wave-11-U)

The X-12 fix correctly subtracts `log_buf_total` and `off_heap_reserved` from
`cache_size` to compute `arbiter_budget`, then clamps to a minimum of 1 MiB:

```rust
let arbiter_budget = (cache_bytes - log_buf_total - off_heap_reserved)
    .max(1024 * 1024_i64);
```

The clamp prevents the arbiter from getting a negative budget, which was the
crash risk. However, if `log_buf_total > cache_bytes`, the log buffers alone
exceed `cache_size` — the total memory use is `log_buf_total + 1 MiB` (arbiter
minimum), which can substantially exceed `cache_size`. The `MemoryBudget` struct
also independently computes `log_buffer_budget` as 7% of `max_memory`, creating
a third independent ceiling that is not wired to the `Arbiter` calculation.

**Failure mode**: A user who sets a small `cache_size` (e.g., 8 MiB) with
default log buffer settings (`log_num_buffers * log_buffer_size = 10 * 1 MiB =
10 MiB`) will run with actual memory use of 11+ MiB while believing the
environment is capped at 8 MiB. No OOM, but the configured budget is exceeded
silently.

**Suggested fix**: When `log_buf_total + off_heap_reserved >= cache_bytes`, emit
a `log::warn!` and optionally clamp `log_num_buffers` or `log_buffer_size` to
fit within `cache_size`. The 1 MiB floor for the arbiter is correct as a
safety net; the diagnostic is missing.

---

### R-7 — X-6 residual: migration WAL write outside BIN latch; fallback to `log_lsn` on WAL failure silently bypasses X-6 crash safety [NEW — MEDIUM]

**Severity**: Medium  
**Subsystem**: noxu-cleaner, noxu-log  
**File:line**: `crates/noxu-cleaner/src/file_processor.rs:340–368`  
**Prior reference**: X-6 (fixed in wave-11-U)

The X-6 fix writes a migration `UpdateLN` WAL entry (`write_migration_ln`)
before inserting the migrated data into the tree. However:

1. **WAL write is outside the BIN latch.** The sequence is: (a) acquire tree
   read lock to fetch data, (b) release tree read lock, (c) call
   `write_migration_ln` with no lock held, (d) acquire tree read lock again to
   `tree.insert`. JE's equivalent holds the BIN write latch across the WAL write
   AND the BIN update atomically. Between steps (b) and (d), a concurrent tree
   write could modify the same key, but the cleaner lock should prevent this.
   **Unverified whether the cleaner lock actually prevents concurrent writes to
   the specific slot** — needs a runtime test.

2. **Fallback to `log_lsn` on WAL failure silently bypasses crash safety.** If
   `write_migration_ln` fails (e.g., `io_invalid` is set, disk full), the code
   falls back to `unwrap_or(log_lsn)`. The tree is then updated with the OLD
   `log_lsn` — the LSN in the file being cleaned. If the file is subsequently
   deleted (after X-5 checkpoint barrier), recovery cannot find the data. The
   `io_invalid` path should prevent further operations, but the code does not
   check `io_invalid` before deciding to proceed with migration; it relies on
   the LWL-path check inside `write_migration_ln`. A failed WAL write during
   migration should abort the migration for this file, not silently fall back.

**Failure mode**: Rare: migration WAL write fails (io_invalid set, disk
pressure) → cleaner proceeds with old LSN → file deleted after checkpoint
barrier → recovery cannot find data. Silent data loss.

**Suggested fix**: If `write_migration_ln` returns `None`, abort the migration
for this entry (return `MigrationOutcome::Locked` to retry later), do not fall
back to `log_lsn`. If `io_invalid` is set, abort the entire cleaning pass.

---

### P-1 — W10 concurrent throughput: `FsyncManager` thundering-herd unresolved; 1.5–2× JE gap persists [LINGERING PERF — HIGH]

**Severity**: High (performance gap vs. stated roadmap goal)  
**Subsystem**: noxu-log fsync manager  
**File:line**: `crates/noxu-log/src/fsync_manager.rs:81–125` (`wait_for_event`, `wakeup_all`)  
**Prior reference**: Wave 11-J (reverted), synthesis H-3 W10 gap

Wave 11-J attempted to replace the `FSyncGroup` condvar with a Treiber-stack
to eliminate the thundering-herd wakeup after `fdatasync`, but the rewrite was
**reverted** because it caused 10–24% regressions across all W10 configurations.
The production code is **unchanged** from the pre-11-J baseline.

The wave-11-J doc identified a minimal alternative that was NOT implemented:

> "A targeted fix for #2 without per-call allocation is feasible by adding an
> `AtomicBool work_done_atomic` to `FSyncGroup` and checking it (no mutex) in
> `wait_for_event` before acquiring `inner` — a ~15 LOC change."

This fast path would eliminate the `Mutex<FsyncGroupInner>` acquisition in
`wait_for_event` for the common case where `work_done = true`. Currently, all
N waiters wake from `condvar.notify_all()` and race to lock `inner` to read
`work_done` — exactly the thundering herd pattern.

**Current benchmark state** (from 11-J wave doc, post-11-I):

| Scale | Threads | Noxu ops/s | JE ops/s | Ratio |
|------:|---------|------------|----------|-------|
| 10K   | 8r/8w   | 3,885      | 10,408   | 2.68× |
| 100K  | 4r/4w   | 3,376      | 4,602    | 1.36× |

The `make benchmarks` acceptance gate (≤1.3× JE at all configurations) is NOT
met. The W10 gap is documented but explicitly deferred with no tracking issue.

**Suggested fix**: Implement the `AtomicBool work_done_atomic` fast path in
`FSyncGroup::wait_for_event` — a ~15 LOC change:

```rust
// In FSyncGroup, add:
work_done_atomic: AtomicBool,

// In wait_for_event, before acquiring inner:
if self.work_done_atomic.load(Ordering::Acquire) {
    return WaitStatus::NoFsyncNeeded;
}
```

Set `work_done_atomic.store(true, Ordering::Release)` inside `wakeup_all` after
setting `inner.work_done`. This eliminates the N-way mutex race without any
per-call allocation.

---

### P-2 — W11 recovery throughput: constant `Environment::open` overhead (~200 ms); acceptance gate still not met [LINGERING PERF — HIGH]

**Severity**: High (performance gap vs. stated roadmap goal)  
**Subsystem**: noxu-dbi environment open, noxu-recovery  
**File:line**: `crates/noxu-dbi/src/environment_impl.rs` (general open path)  
**Prior reference**: Wave 11-K, W11 gap

Wave 11-K correctly identified and fixed the per-record allocation in `redo_ln`
(3 allocation types eliminated), but the benchmark showed no measurable
improvement: W11 remains at **2.9× JE** at 100K records (target ≤1.5×). The
wave-11-K doc diagnosed the dominant cost as a ~200 ms constant
`Environment::open` overhead, not the redo loop:

| Component | Time estimate |
|---|---:|
| `Environment::open` setup | ~120 ms |
| `find_end_of_log` + `find_last_checkpoint` | ~30 ms |
| `run_analysis` | ~15 ms |
| `run_redo` (100K inserts) | ~25 ms |

The **accepted path to close the W11 gap** — BIN deserialization from
`dirty_in_map` to avoid replaying individual LN records during recovery — was
identified in 11-K but never implemented. At 100K records and a clean close,
ALL data is already in the checkpointed BINs; `run_redo` should be near-O(0)
for this case.

Additionally, `run_analysis` calls `scan_forward` which collects ALL WAL entries
into a `Vec<PositionedEntry>` before the redo pass. This is an O(N) allocation
proportional to the WAL size. 11-K recommended streaming analysis (`scan_forward_fn`
callback) to avoid the intermediate Vec.

**Suggested fix** (from 11-K): Implement BIN restoration from `dirty_in_map` in
`run_redo`. For each checkpointed BIN present in `dirty_in_map`, deserialize the
BIN entry directly (skipping individual LN replay). New LNs written AFTER the
last checkpoint are applied normally. This would make recovery O(BIN_count +
new_LN_count) rather than O(all_LN_count).

---

### P-3 — `path.metadata()` stat-syscall on every `write_buffer` invocation [LINGERING PERF — MEDIUM]

**Severity**: Medium  
**Subsystem**: noxu-log file manager  
**File:line**: `crates/noxu-log/src/file_manager.rs:538`  
**Prior reference**: Keith original audit F-2.3 (not fixed in any wave)

`write_buffer` calls `path.metadata()` on every invocation to check whether the
file has exceeded `max_file_size` and needs to flip:

```rust
// file_manager.rs:536–543
let path = self.file_path(file_num);
let file_len = path.metadata().map(|m| m.len()).unwrap_or(0);
if file_len >= self.max_file_size {
    self.flip_file()?;
}
```

This is one `fstat()` syscall per call to `write_buffer`, which is called once
per `flush_sync`/`flush_no_sync` invocation (under the LWL). At 10,000
transactions/second this is 10,000 `fstat()` syscalls per second on the write
hot path.

`write_buffer` already knows how many bytes it wrote (`data.len()`), and
`LogManager` already tracks `next_available_lsn` (which encodes the current file
position). The file flip decision can be made by comparing
`next_available_lsn.file_offset() + data.len()` against `max_file_size` without
any syscall.

**Failure mode**: Marginal CPU on every commit path; on real NVMe with sub-ms
commit latency the `fstat()` overhead becomes a measurable fraction.

**Suggested fix**: Track `current_file_written_bytes: AtomicU64` in
`FileManager`; increment by `data.len()` in `write_buffer`; replace the
`metadata()` call with an atomic load and compare.

---

### S-1 — X-5 checkpoint barrier: `process_checkpoint_end` runs every checkpoint with O(N_cleaned_files + N_checkpointed_files) scan [PERF NOTE — LOW]

**Severity**: Low  
**Subsystem**: noxu-cleaner file selector  
**File:line**: `crates/noxu-cleaner/src/file_selector.rs:450–469` (`process_checkpoint_end`);
`crates/noxu-recovery/src/checkpointer.rs:530–537`

`process_checkpoint_end` is called from `Checkpointer::do_checkpoint` on every
successful checkpoint. It iterates `self.checkpointed` (a `HashSet<u32>`) to
advance files to `safe_to_delete`, and iterates `state.cleaned_files` (a
`Vec<u32>`) to advance cleaned files to checkpointed:

```rust
let already_checkpointed: Vec<u32> =
    self.checkpointed.iter().copied().collect(); // O(N_checkpointed)
for file_number in already_checkpointed {
    self.mark_file_fully_processed(file_number);
}
for &file_number in &state.cleaned_files {      // O(N_cleaned)
    if self.cleaned.contains(&file_number) { … }
}
```

Both loops are inside `self.file_selector.lock()` (held by `after_checkpoint`
via `cleaner.after_checkpoint`). At 10x file counts (e.g., 10,000 log files
under sustained write load), `N_checkpointed` + `N_cleaned` can be in the
hundreds. The entire operation runs inside the `file_selector` mutex, which also
blocks concurrent cleaner operations.

This is not a correctness issue and at typical scale (tens of files) the cost is
negligible. But the design does not bound the checkpoint callback latency.

**Suggested fix**: Cap `N_files_per_checkpoint_pass` or use a generation counter
instead of iterating all files: maintain `safe_to_delete_generation: u64` and
promote all files whose generation ≤ current checkpoint generation in O(1) with
a VecDeque sorted by generation.

---

### S-2 — `db_trees_registry` O(N) linear scan in `commit_pending_database` [NEW — LOW]

**Severity**: Low  
**Subsystem**: noxu-dbi environment  
**File:line**: `crates/noxu-dbi/src/environment_impl.rs:1122–1144`

Noted inline with R-4 above. Both `commit_pending_database` and
`abort_pending_database` perform `db_map.read().iter().find_map(...)` — an O(N)
linear scan over all open databases — to look up `db_id` by name. At 10x scale
(hundreds of open databases), this runs during the transaction commit callback
for every database-creation commit, holding the `pending_names.write()` lock.

**Suggested fix**: Change `pending_names: RwLock<HashSet<String>>` to
`pending_names: RwLock<HashMap<String, DatabaseId>>`. Store the assigned `db_id`
when inserting into `pending_names` in `open_database_inner`. Eliminate the
linear scan in `commit_pending_database`.

---

### S-3 — FSyncGroup `take_error()` clones error `String` inside mutex (lingering F-6.4) [LOW]

**Severity**: Low  
**Subsystem**: noxu-log fsync manager  
**File:line**: `crates/noxu-log/src/fsync_manager.rs:113–116`  
**Prior reference**: Keith F-6.4 (not fixed in any wave)

Still present. After a failed `fdatasync`, all N concurrent waiters call
`take_error()` which clones a `String` while holding the `FSyncGroup::inner`
mutex. At N=8 this serialises 8 `String` allocations under the same mutex.

**Suggested fix**: Change `error: Option<String>` to `error: Option<Arc<str>>`
in `FsyncGroupInner`. `clone()` becomes an atomic refcount increment.

---

### C-1 — W11 crash recovery performance: `find_end_of_log` O(N_files) directory scan [LINGERING — MEDIUM]

**Severity**: Medium  
**Subsystem**: noxu-log, noxu-recovery  
**File:line**: `crates/noxu-log/src/file_manager.rs:222–238` (`list_file_numbers`)  
**Prior reference**: Keith F-4.5 (partially noted; not fixed in any wave)

`list_file_numbers()` performs a full `fs::read_dir()` + sort on every call.
`find_end_of_log` is called during every recovery open. With 10,000 log files
this is an O(N log N) directory scan plus heap sort at startup. The 11-K wave
doc lists `find_end_of_log + find_last_checkpoint` at ~30 ms of the ~200 ms
constant overhead.

This is distinct from the F-4.5 "cleaner calls it per-pass" finding (which may
also still be present) — specifically calling it out here because it directly
contributes to the W11 gap that 11-K failed to close.

**Suggested fix**: Persist the last-known log file number in a small manifest
file (`noxu.manifest`) that is atomically updated on each `flip_file`. Recovery
reads the manifest instead of scanning the directory. Dirty recovery (crash
before manifest write) falls back to the directory scan. This is what JE's
`FileManager` does with its `envImpl.getCleaner().getFileSelector()` state.

---

## Summary Table

| ID | Severity | Subsystem | File:line | Failure Mode | Prior? |
|---|---|---|---|---|---|
| **R-1** | **High** | noxu-log | `log_manager.rs:594` | Alloc pressure every commit flush | H-3/F-1.2 partial |
| **R-2** | **High** | noxu-dbi/noxu-log | `log_manager.rs:568`, `environment_impl.rs:822` | Commit latency spikes when LogFlushTask fires | New (X-11 impl) |
| **R-3** | **Medium** | noxu-db XA, noxu-rep | `environment.rs:1503`, `replicated_environment.rs:2102` | VLSN lost after second crash post-XA resolution | X-3 residual |
| **R-4** | **Medium** | noxu-dbi | `environment_impl.rs:1122` | TOCTOU phantom DB / O(N) scan under lock | New (C-4/C-6 impl) |
| **R-5** | **Medium** | noxu-dbi, noxu-recovery | `environment_impl.rs:963`, `recovery_manager.rs:246` | Crash-aborted DB creation recovered as committed | C-6 partial |
| **R-6** | **Low** | noxu-dbi | `environment_impl.rs:557` | Silent memory budget overrun | X-12 residual |
| **R-7** | **Medium** | noxu-cleaner | `file_processor.rs:340` | Migration WAL fail → silent crash-safety bypass | X-6 residual |
| **P-1** | **High** | noxu-log | `fsync_manager.rs:81` | W10 throughput 1.5–2.7× JE; ~15 LOC fix available | 11-J reverted |
| **P-2** | **High** | noxu-dbi, noxu-recovery | env open path | W11 2.9× JE; acceptance gate NOT met | 11-K partial |
| **P-3** | **Medium** | noxu-log | `file_manager.rs:538` | `fstat()` on every write_buffer invocation | F-2.3 unlanded |
| **S-1** | **Low** | noxu-cleaner | `file_selector.rs:450` | O(N_files) under lock every checkpoint | New (X-5 impl) |
| **S-2** | **Low** | noxu-dbi | `environment_impl.rs:1122` | O(N_db) scan on db creation commit | New (C-4/C-6 impl) |
| **S-3** | **Low** | noxu-log | `fsync_manager.rs:113` | String alloc under mutex on fsync fail | F-6.4 unlanded |
| **C-1** | **Medium** | noxu-log | `file_manager.rs:222` | O(N_files) dir scan on every recovery open | F-4.5 residual |

---

## Severity Counts

| Severity | Count |
|---|---|
| High | 4 (R-1, R-2, P-1, P-2) |
| Medium | 5 (R-3, R-4, R-5, R-7, C-1, P-3) |
| Low | 4 (R-6, S-1, S-2, S-3) |
| **Total** | **13** |

---

## Top 5 Critical/High Findings

1. **R-2 (High)** — LogFlushTask daemon holds LWL across `pwrite64`. Every
   background flush injects a blocking I/O stall into ALL concurrent transaction
   commits. This is a direct regression introduced by the X-11 implementation in
   wave-11-U. The fix (release LWL before I/O in the background path) is
   straightforward and non-breaking.

2. **P-1 (High)** — W10 concurrent throughput 1.5–2.7× JE with a documented
   ~15 LOC fix (`AtomicBool work_done_atomic` in FSyncGroup) that wave-11-J
   identified but never shipped because the full Treiber-stack rewrite was
   abandoned. The minimal fix has zero allocation overhead and was not in the
   revert.

3. **P-2 (High)** — W11 recovery 2.9× JE with acceptance gate explicitly not
   met. The `Environment::open` constant overhead dominates; BIN restoration
   from `dirty_in_map` is the accepted solution but has not been implemented.

4. **R-1 (High)** — `collect_dirty_buffers` still allocates a `Vec<(Vec<u8>, u64)>`
   per flush. The wave-11-S H-3 fix addressed the per-entry encoding allocation
   but left the per-buffer copy allocation untouched. This is measurable in every
   profile run and the fix (reuse the buffer list as part of the LWL state) is
   directly analogous to the H-3 fix already shipped.

5. **R-4 (Medium)** — `commit_pending_database` TOCTOU race plus O(N) linear
   scan. The C-4/C-6 commit path (wave-11-R/U) correctly uses transaction
   callbacks but introduced a gap between `pending_names` removal and `name_map`
   insertion where a concurrent `open_database` can race. At scale the O(N) scan
   under the write lock is independently problematic.

---

## Top 3 Performance Findings

1. **P-1** — `FsyncGroup` thundering-herd: `notify_all` wakes all N waiters
   into a mutex race on `FsyncGroupInner`. Minimal fix documented, not
   implemented. At 8+ concurrent writers this is the primary W10 throughput
   bottleneck.

2. **P-2** — W11 recovery 2.9× JE: BIN deserialization from `dirty_in_map`
   (the only known path to close the gap) never implemented. The 11-K redo-loop
   allocation reduction saved at most ~8 ms of a ~254 ms constant.

3. **R-2** — LogFlushTask holds LWL across `pwrite64`. On a disk with any
   non-negligible write latency, enabling `log_flush_no_sync_interval_ms > 0`
   causes periodic commit latency spikes correlated with the flush interval.
   This defeats the purpose of the feature (bounding `NoSync` commit latency)
   by potentially increasing p99 latency for ALL writers.

---

## Top 3 Crash-Safety Findings

1. **R-7 (Medium)** — X-6 migration fallback silently bypasses crash safety.
   `write_migration_ln` failing (e.g., disk pressure during cleaning) causes
   the cleaner to proceed with the original `log_lsn`, keeping a stale LSN
   pointing to a file that will be deleted. On crash recovery, the data cannot
   be found. The WAL failure should abort the migration, not silently continue.

2. **R-3 (Medium)** — Recovered XA commit VLSN lost after second crash. The
   X-3 fix registers the VLSN in the in-memory `VlsnIndex` but writes
   `NULL_VLSN` to the TxnCommit WAL entry. The X-14 VLSN rebuild on recovery
   does not scan TxnCommit entries. After a second crash, the XA commit's VLSN
   is invisible to the replication layer; replicas may never receive those
   writes without a network restore.

3. **R-5 (Medium)** — C-6 partial fix: for the non-transactional
   `open_database(None, ...)` path, `NameLN` is written WITHOUT a `txn_id`.
   The `run_mapping_tree_undo_pass` treats `txn_id=None` as committed. A crash
   that aborts the surrounding user logic still leaves the database registered
   in the WAL as committed. The C-4 fix added callbacks; the C-6 write-path
   change was only applied to the transactional variant.

---

## Confirmed Closed (NOT re-reported)

The following prior high-severity findings were verified closed in `origin/main`
and are NOT re-reported:

- C-1/F-3.1 parent-dir fsync: `file_manager.rs` now `sync_all()`'s parent dir.
- C-2/F-3.2 fsync invalidates env: `io_invalid AtomicBool` set on fdatasync error; checked at all `log()` entry points AND at `Database::check_open()` / `CursorImpl::check_state()`.
- C-3/F-3.5 CRC in recovery scanner: `parse_entry_from_bytes` now validates CRC32.
- H-1/F-2.2 env lock across abort: `transaction.rs::abort()` no longer holds env lock during BTree undo.
- H-2/F-6.2 lock ordering: canonical shard-before-waiter-graph order established.
- X-5 cleaner checkpoint barrier: `after_checkpoint` wired into `do_checkpoint`.
- X-13 io_invalid propagates to reads: `check_open` + `check_state` both check `io_invalid`.
- X-4 `resolving_xids` sentinel: TOCTOU window in XA commit resolved by sentinel set.
- X-15 open-ended rollback: `pending_rollback_starts` in `RollbackTracker` covers incomplete intervals.

---

*Output file*: `/tmp/noxu-reaudit-keith.md`  
*Findings*: 13 total — 4 high, 5 medium, 4 low  
*Key new issues*: R-2 (LogFlushTask LWL-across-IO), R-4 (commit_pending TOCTOU)  
*Key lingering*: P-1 (W10 gap), P-2 (W11 gap), R-1 (collect_dirty_buffers alloc)
