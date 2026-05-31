# Wave ZC — Crash-Safety Holes and Performance Regressions

**Target**: v3.1.0  
**Branch**: `fix/zc-crash-perf`  
**Reviewer source**: Keith Bostic re-audit (`/tmp/noxu-reaudit-keith.md`)  
**Subsystems**: noxu-log, noxu-cleaner, noxu-recovery, noxu-rep, noxu-db, noxu-dbi

---

## Summary

Addresses 7 findings from Keith's re-audit: R-1, R-2, R-3, R-5, R-7
(correctness/crash-safety) and P-1, P-2 (performance).

| Item | Finding | Severity | Status |
|---|---|---|---|
| 1 | R-2 — LWL held across pwrite64 in LogFlushTask | HIGH | **Fixed** |
| 2 | R-1 — collect_dirty_buffers per-buffer allocation | HIGH | **Partial** |
| 3 | R-7 — cleaner migration silent fallback to stale LSN | MEDIUM/crash | **Fixed** |
| 4 | R-3 — recovered XA TxnCommit NULL_VLSN in WAL | MEDIUM/crash | **Fixed** |
| 5 | R-5 — non-txn NameLN recovery invariant | MEDIUM | **Documented+Tested** |
| 6 | P-1 — FsyncGroup thundering-herd | HIGH/perf | **Fixed** |
| 7 | P-2 — W11 recovery 2.9× JE gap | HIGH/perf | **Design note** |

---

## Item 1 — R-2: LWL released before pwrite64 in `flush_no_sync`

**Finding**: The `LogFlushTask` background daemon called `flush_no_sync()`,
which held the log-write-latch (LWL) across all `pwrite64` syscalls.  On a
disk with 2–5 ms per write, every background flush injected a blocking stall
into ALL concurrent foreground transaction commits.

**Fix** (`crates/noxu-log/src/log_manager.rs`):

Restructured `flush_no_sync()` to:
1. Acquire LWL: snapshot dirty-buffer data into `pending_snapshot` and capture `eol`
2. Release LWL (before any I/O)
3. Issue `write_buffer()` calls outside the LWL

**Correctness argument**:

`fill_flush_pending()` (formerly `collect_dirty_buffers`) advances each
buffer's `flushed_len` watermark under the per-buffer latch before returning.
After that advance:
- Concurrent foreground writers may only append at positions ≥ `new_flushed_len`
  — strictly after the range we snapshotted.
- The `pwrite64` calls therefore write to disjoint file regions from any
  concurrent foreground write.
- `write_buffer()` serialises internally via its file-handle lock.

**Contrast with `flush_sync`**: the foreground commit path intentionally holds
the LWL through `pwrite64` so that all concurrent committers complete kernel
writes before entering `FsyncManager` together, enabling fsync coalescing.  The
background flush has no such coalescing requirement.

**Note**: If `write_buffer()` fails after the LWL is released, `mark_flushed()`
has already been called on the affected buffers.  The error propagates to
`LogFlusher`, which logs it and retries; the environment is invalidated via
`io_invalid` as in the prior implementation.

---

## Item 2 — R-1: `collect_dirty_buffers` per-buffer allocation (partial fix)

**Finding**: `collect_dirty_buffers` allocated a fresh `Vec<(Vec<u8>, u64)>` on
every `flush_sync`/`flush_no_sync` call, plus a `to_vec()` copy per dirty buffer.

**Fix** (`crates/noxu-log/src/log_manager.rs`):

Introduced `LwlScratch` struct grouping the existing entry-encoding scratch
buffer (`entry_buf`) with a new reusable flush list (`flush_pending`).
Changed `log_write_latch: Mutex<Vec<u8>>` to `Mutex<LwlScratch>`.

- **`flush_sync` (hot path)**: iterates `guard.flush_pending` while holding the
  LWL; `clear()` retains Vec capacity between calls.  After warm-up (typically
  the first few flushes), zero outer-Vec allocations per flush.
- **`flush_no_sync` (background, infrequent)**: uses `std::mem::take` to move
  items out before releasing the LWL (required by R-2); outer-Vec capacity is
  lost on each call (acceptable since background flush is infrequent).

**Limitation — inner `to_vec()` copy remains**:

The per-buffer `to_vec()` is unavoidable while releasing the LWL before I/O
(R-2 requirement).  Zero-copy would require either:
1. Holding the buffer latch through `write_buffer()` (conflicts with R-2), or
2. Reference-counting the buffer data (e.g., `Arc<[u8]>` or `bytes::Bytes`
   via `BytesMut::split_to`) — this empties the buffer and requires
   reallocation on the next write cycle, which is not net-positive for the
   pool's hot steady-state.

The outer-Vec reuse in `flush_sync` addresses the allocation Keith cited in
H-3/F-1.2.  The inner copy cost is the price of releasing the LWL before I/O;
the trade-off is documented here for the next wave that considers buffer
reference counting.

---

## Item 3 — R-7: Cleaner migration abort on WAL write failure

**Finding** (X-6 residual): `SharedTreeLookup::migrate_ln_slot` fell back to
the original `log_lsn` when `write_migration_ln()` returned `None` (WAL write
failure due to `io_invalid`, disk full, etc.).  That stale LSN points to the
file being cleaned; if the file is subsequently deleted (after the X-5
checkpoint barrier), recovery cannot find the data.

**Fix** (`crates/noxu-cleaner/src/file_processor.rs`):

Changed the `None` branch from `unwrap_or_else(|| self.log_manager.get_end_of_log())`
to:
1. Release the cleaner lock (clean up)
2. Return `MigrationOutcome::Locked` — entry will be retried on the next pass

The source file remains protected by the X-5 checkpoint barrier until a
successful WAL write occurs.

**Test**: `test_r7_migration_abort_on_wal_write_failure` — sets `io_invalid`
on the LogManager, confirms `Locked` is returned, and verifies the tree slot
retains the original `log_lsn`.

---

## Item 4 — R-3: Recovered XA TxnCommit VLSN in WAL entry

**Finding** (X-3 residual): `write_txn_commit_for_recovered()` wrote the
`TxnCommit` WAL entry with `NULL_VLSN` in the `dtvlsn` payload field, then
registered a real VLSN in the in-memory `VlsnIndex`.  After a second crash
before the next `vlsn.idx` flush, the X-14 VLSN rebuild (which only scanned
LN records) missed the TxnCommit entry → VLSN lost → replicas could not converge.

**Fix (two parts)**:

1. **Pre-allocate VLSN before writing WAL entry** (`noxu-db/src/environment.rs`,
   `noxu-dbi/src/replica_ack.rs`, `noxu-rep/src/replicated_environment.rs`):
   - Added `pre_alloc_vlsn_for_recovered_commit() -> u64` to `ReplicaAckCoordinator`
     trait (default = 0; `ReplicatedEnvironment` increments the latest VLSN
     without registering it).
   - Added `register_recovered_commit_vlsn(vlsn, commit_lsn)` to trait (default
     = no-op; `ReplicatedEnvironment` registers in `VlsnIndex`).
   - `write_txn_commit_for_recovered()`: pre-alloc → write entry with VLSN in
     `dtvlsn` field → register with actual commit LSN.

2. **X-14 VLSN rebuild includes TxnCommit-derived VLSNs** (`noxu-recovery`):
   - `TxnCommitRecord` gains `dtvlsn: Option<u64>`.
   - `file_manager_scanner.rs` populates `dtvlsn` from `TxnEndEntry.dtvlsn`.
   - `AnalysisResult` gains `txncommit_vlsns: Vec<(u64, u64)>`.
   - Analysis pass collects `(vlsn, commit_lsn)` from TxnCommit records with
     non-zero `dtvlsn`.
   - Both redo VLSN rebuild paths extend from `txncommit_vlsns` before
     sort+dedup.

**Test**: `test_r3_txncommit_dtvlsn_in_recovered_vlsns` — TxnCommit with
`dtvlsn=42` appears in `recovered_vlsns` after recovery.

---

## Item 5 — R-5: Non-transactional NameLN recovery invariant

**Finding** (C-6 partial): Keith noted that `run_mapping_tree_undo_pass()`
treats NameLNs with `txn_id=None` as committed.  The question was whether this
is correct.

**Resolution**: The behavior is correct.  Non-transactional `open_database(None,
...)` writes a `NameLN` entry without a `txn_id` at call time.  There is no
wrapping transaction to abort — the write is immediately durable.  Recovery
correctly treats it as committed.

**Fix**: Documentation and test only.
- `run_mapping_tree_undo_pass()` doc comment expanded with explicit R-5 and C-6
  invariant explanations.
- Test `test_r5_non_txn_namelns_always_survive_recovery` pins the invariant:
  a NameLN with `txn_id=None` always survives recovery, even when other
  transactions have aborted.

---

## Item 6 — P-1: FsyncGroup thundering-herd AtomicBool fast path

**Finding**: After a completed fsync, `condvar.notify_all()` woke all N
waiters into a `Mutex<FsyncGroupInner>` race to read `work_done`.  At 8+
concurrent writers this thundering-herd was the primary W10 throughput
bottleneck.  Wave 11-J identified a ~15 LOC fix but never shipped it (the
larger Treiber-stack rewrite was reverted).

**Fix** (`crates/noxu-log/src/fsync_manager.rs`):

Added `work_done_atomic: AtomicBool` to `FSyncGroup`:
- `wakeup_all()` and `wakeup_all_with_error()`: store `true` (Release) into
  the atomic BEFORE acquiring `inner` mutex.
- `wait_for_event()`: checks `work_done_atomic.load(Acquire)` BEFORE acquiring
  `inner`.  If `true`, returns `NoFsyncNeeded` immediately.

After a completed fsync, all N waiters see `work_done_atomic=true` via a
single atomic load and return without ever acquiring the mutex.  The N-way
mutex race is eliminated.

**Tests**: All existing fsync property tests pass including
`test_fsync_before_commit_invariant` (8 threads × 200 commits).

---

## Item 7 — P-2: W11 recovery 2.9× JE — Design note

**Finding**: Recovery throughput is 2.9× JE at 100K records.  The dominant
cost (~120 ms of ~254 ms constant) is `Environment::open` setup overhead, not
the redo loop.

**Approach identified in Wave 11-K** (not implemented):

> Restore BINs from the `dirty_in_map` (checkpoint-serialized tree nodes)
> instead of replaying individual LN records.  For a cleanly-closed environment,
> ALL data is in the checkpointed BINs; `run_redo` should be near-O(0).

**Estimated effort**: Large (2–3 wave-equivalents).  Requires:
1. Deserializing BIN entries directly from the checkpoint (bypassing LN replay).
2. Applying post-checkpoint LNs (written after the last checkpoint) normally.
3. Changes to `RecoveryManager::run_redo_all()`, `DirtyINMap`, and `EnvironmentImpl`.

**Secondary optimization**: `run_analysis` calls `scan_forward` which collects
all WAL entries into an intermediate `Vec<PositionedEntry>`.  Converting to a
streaming callback (`scan_forward_fn`) would reduce the ~30 ms
`find_end_of_log + find_last_checkpoint` overhead.

**Recommendation**: Schedule as a dedicated wave (Wave ZD or equivalent).
The BIN-restore approach was validated in Wave 11-K analysis and is the only
known path to close the W11 gap (≤1.5× JE target).

---

## Final Gate Results

```
cargo fmt --all -- --check             ✓
cargo clippy --workspace -- -D warnings  ✓
cargo test --workspace --no-fail-fast  ✓ (all tests pass)
make docs-check                        ✓
```

Recovery/log/cleaner test results after all changes:
- `noxu-log`: 13 passed
- `noxu-recovery`: 203 passed  
- `noxu-cleaner`: 360 passed
- `noxu-db`: 71 passed
- `noxu-dbi`: 89 passed

---

## Files Changed

| File | Items |
|---|---|
| `crates/noxu-log/src/log_manager.rs` | R-1, R-2 |
| `crates/noxu-log/src/fsync_manager.rs` | P-1 |
| `crates/noxu-cleaner/src/file_processor.rs` | R-7 |
| `crates/noxu-dbi/src/replica_ack.rs` | R-3 (trait) |
| `crates/noxu-rep/src/replicated_environment.rs` | R-3 (impl) |
| `crates/noxu-db/src/environment.rs` | R-3 (call site) |
| `crates/noxu-recovery/src/log_scanner.rs` | R-3 (TxnCommitRecord) |
| `crates/noxu-recovery/src/recovery_manager.rs` | R-3, R-5 |
| `crates/noxu-recovery/src/analysis_result.rs` | R-3 |
| `crates/noxu-dbi/src/file_manager_scanner.rs` | R-3 |
