# JE Algorithmic-Fidelity Audit — Cleaner, Transactions, Locking, Concurrency (2026-06)

Noxu DB is a Rust port of Berkeley DB Java Edition (JE) and is intended to be a
faithful re-creation of JE's algorithms. This audit compares the Noxu
implementation against the JE reference (`../je/src/com/sleepycat/je/`) in four
subsystems — the log cleaner, transaction management, the lock manager, and
B-tree/evictor/daemon concurrency — and records every observed algorithmic
divergence, classified as:

- **JUSTIFIED** — a deliberate Rust-idiom adaptation or a documented design
  decision (the concurrency *primitive* differences — `parking_lot` vs Java
  `ReentrantReadWriteLock`, `std` atomics, single-threaded daemons — are
  justified by `AGENTS.md` and are not re-listed individually).
- **DRIFT** — an unintended difference that is likely a bug and should be
  corrected (or, if intentional, documented).

This is a read-only audit. No code was changed. Findings are recorded here for
triage; fixes are tracked separately.

> **Scope note.** The audit covers algorithm/ordering fidelity, not
> line-for-line style. Conflict matrices and grant-type tables were compared
> cell-for-cell; descent/eviction/cleaning paths were compared step-by-step.

## Headline assessment

The **conflict and lock-upgrade matrices, lock grant types, waiter ordering,
the WaitRestart→RangeRestart path, commit/abort lock-release ordering, and the
core tree-descent latch-coupling are all faithful to JE.** The lock manager is
in particularly good shape.

The most serious divergences are concentrated in three places:

1. **Cursor stability across BIN splits** (concurrency D-2): Noxu does **not**
   reposition open cursors when a BIN splits under them. JE calls
   `BIN.adjustCursors(newSibling, low, high)` inside every split
   (`IN.java:4259`); Noxu's `split_child` has no equivalent. A cursor iterating
   a BIN that splits can silently skip or revisit records.
2. **Cleaner safe-to-delete soundness** (cleaner DRIFT-1/2/3 + the
   checkpointer interaction): the cleaner lacks the `pendingLNs`/`pendingDBs`
   gating that JE uses to keep a file from being deleted before its migrated
   entries are durable, and it has no abort/put-back path for a file whose
   processing fails. Compounding this, the checkpointer only flushes the
   internal `primary_tree`, not user-database BINs (a previously-recorded
   limitation; see `wave-gb-dbtree-recovery.md` and the St-H6 finding), so the
   two-checkpoint safe-to-delete barrier does not by itself guarantee that no
   live BIN slot points into a deleted file.
3. **Transaction-manager bookkeeping** (txn DRIFT-4/5): the serializable-active
   counter and the explicit-txn unregister are not wired the way JE wires
   them, so the evictor never learns that serializable transactions are active
   and `all_txns` / `n_active_txns()` drift over time.

Each is detailed below with JE and Noxu locations.

## Verification status

The two highest-impact findings were independently re-checked against the
source after the audit:

- **D-2 confirmed.** `noxu-tree/src/bin.rs` has a `cursor_set:
  Option<HashSet<u64>>` and `add_cursor`/`get_cursor_set`, but the only
  split-time cursor operation is `adjust_cursor_count` (a counter). There is no
  `adjust_cursors` anywhere in the crate, while JE calls
  `adjustCursors(newSibling, low, high)` from `IN.split` (`IN.java:4259`).
- **Cleaner DRIFT-1 confirmed.** `noxu-cleaner` exposes `pending_lns_*` *stats*
  (`cleaner_stat.rs`) but there is no `pending_lns` map, no `process_pending`,
  and no `put_back_file_for_cleaning` in the cleaner source.

The remaining findings are recorded as reported by the audit and should be
re-verified at fix time.

---

## A. Lock manager + transaction management

### Faithful (verified)

- **Lock conflict matrix** (5×5: READ/WRITE/RANGE_READ/RANGE_WRITE/RANGE_INSERT)
  — identical to JE `LockType.java` cell-for-cell (`noxu-txn/src/lock_type.rs`).
- **Lock upgrade matrix** (5×5) — identical to JE.
- **Grant types** — all 8 (`New`, `WaitNew`, `Promotion`, `WaitPromotion`,
  `Existing`, `Denied`, `WaitRestart`, `NoneNeeded`) present with matching
  semantics; WAIT_PROMOTION goes to head-of-waiter-list, WAIT_NEW/WAIT_RESTART
  to the end — matches JE.
- **WaitRestart → RangeRestart** — the waiter is typed `Restart` and the wakeup
  returns `Err(RangeRestart)`; matches JE (this was the T-F2 fix).
- **Commit/abort lock-release ordering** — read locks released, log, then write
  locks; abort sets `Aborted` before undo. Matches JE.
- **Isolation → lock-lifetime** — read-committed releases the read lock
  per-record in `cursor_impl::lock_ln`; serializable acquires `RangeRead`; all
  read locks drain at commit (JE releases read locks at commit too). Faithful.
- **Thin→full lock mutation**, **WriteLockInfo fields**, **locker hierarchy**
  (`BasicLocker`/`ThreadLocker`/`HandleLocker`) — faithful (one gap, DRIFT-6).
- **Lock-table shard count** 64 vs JE's configurable-default 16 — JUSTIFIED
  (config eliminated; tuning constant; no correctness impact).

### DRIFT

| ID | Severity | JE | Noxu | Issue |
|---|---|---|---|---|
| TXN-1 | deadlock latency | `LockManager.waitForLock()` checks deadlock every wakeup | `lock_manager.rs::lock_with_sharing_and_timeout` | Deadlock re-check is gated on `timed_out.timed_out()` (only on the 50 ms slice), not on every wakeup as in `lock_with_timeout`. A cycle forming on the sharing path can go undetected up to 50 ms. Fix: make the re-check unconditional. |
| TXN-2 | correctness | `TxnManager.registerTxn`/`unRegisterTxn` maintain `nActiveSerializable` | `txn_manager.rs` begin/commit/abort | `register_serializable`/`unregister_serializable` exist and are tested but never called, so `are_other_serializable_transactions_active()` is always false and the **evictor never learns serializable txns are active** (may evict BINs a serializable cursor depends on). Already noted in `review-txn-isolation-2026-06.md`. |
| TXN-3 | correctness | `Txn.close()` → `unRegisterTxn` | `txn.rs` commit/abort | Confirmed continuation of T-F5: `Txn` commit/abort do not call back to `TxnManager`, so `all_txns` grows and `n_active_txns()`/`get_first_active_lsn()` drift. |
| TXN-4 | correctness (minor) | `CursorImpl.lockLN` calls `locker.lock(NONE)` → `checkState()` even for dirty reads | `cursor_impl.rs::lock_ln` (~1017) | Read-uncommitted early-returns before validating the locker state, so a `MustAbort`/`Aborted` txn doing a dirty read is not caught at the lock point. Fix: `check_state()` before the early return. |
| TXN-5 | correctness (rare) | `HandleLocker.sharesLocksWith` covers the non-txn buddy | `handle_locker.rs::shares_locks_with` | Only the transactional-buddy case is handled; a `ThreadLocker`-opened DB handle won't share with its non-transactional opener (can self-conflict on the NameLN during `Database.open`). |
| TXN-6 | cosmetic / spec | `DeadlockChecker.chooseTargetedLocker` picks an identity-hash pseudorandom victim from the sorted-by-thread-id cycle | `deadlock_detector.rs::select_victim` | Noxu uses "fewest locks held, then youngest." Any cycle-breaking victim is *correct*, so this is not a bug, but it is an undocumented divergence from JE's algorithm; document it or align it. |

`get_first_active_lsn` always returning `NULL_LSN` is **JUSTIFIED** (T-F4 —
deliberately unwired because bounding the recovery scan is unsafe under the
current checkpointer; documented in the method rustdoc).

---

## B. Cleaner

### Faithful (verified)

`FileSummary` obsolete-size estimation, the `LookAheadCache`, `process_found_ln`
(the four LSN-comparison cases), `process_in`/`process_bin_delta`,
`TrackedFileSummary` copy-on-write offset accumulation, `FileProtector`
reference counting, `PackedOffsets` encoding, the `FileStatus` five-state enum,
and `PROCESS_PENDING_EVERY_N_LNS = 100` all match JE.

### DRIFT (ranked)

| ID | Severity | Issue |
|---|---|---|
| CLN-1 | **data-loss / correctness** | No `pendingLNs`/`pendingDBs` sets, no `anyPendingDuringCheckpoint`, no `process_pending`. When an LN migration is denied a lock, JE keeps the file un-deletable (retried via `processPending`) until the pending set drains; Noxu has no such gate, so a file whose live LN could not be migrated can still be advanced toward deletion → dangling BIN slot after a crash. (JE: `FileSelector.java` pending sets + `Cleaner.processPending`.) |
| CLN-2 | correctness (latent) | `CheckpointStartCleanerState` snapshots only `cleaned_files`, not the `FULLY_PROCESSED` set; once CLN-1 is fixed the two-checkpoint barrier is structurally incomplete. (JE: `FileSelector.getFilesAtCheckpointStart`.) |
| CLN-3 | correctness (stuck state) | No `put_back_file_for_cleaning` / `finally`-equivalent: if `process_single_file` errors or shuts down mid-run, the file is stuck in `BEING_CLEANED` forever (JE's `doClean` `finally` moves it back to `TO_BE_CLEANED`). |
| CLN-4 | correctness | File selection ignores `firstActiveTxnLsn`; under a long-running transaction Noxu can clean a file still inside the active-txn window (JE clamps `firstActiveFile = min(newest, firstActiveTxnFile)`). |
| CLN-5 | correctness / perf | Two-pass cleaning is inverted: JE runs a read-only first pass and *skips* cleaning if true utilization is still above `requiredUtil`; Noxu lowers the threshold and force-cleans. Over-cleaning under TTL workloads with high expiration uncertainty. |
| CLN-6 | correctness / perf | File selection uses a per-file min-utilization threshold only; JE gates on *predicted total* utilization (`predictedMinUtil < totalThreshold`) plus a `minFileUtilization` tier. Under/over-cleaning relative to policy. |
| CLN-7 | correctness (latent) | `get_checkpoint_state` doesn't call `process_pending` first (moot until CLN-1). |
| CLN-8 | perf / completeness | `FilesToMigrate` (`je.cleaner.forceCleanFiles`) not implemented. |
| CLN-9 | correctness / perf | `ExpirationProfile` is a global stub, not JE's per-file persistent histogram store; TTL-adjusted file selection can't be computed correctly. |
| CLN-10 | correctness | Expiration units: JE uses packed integer **hours** throughout; Noxu's `ExpirationTracker`/`LnInfo` use raw `u64` (documented as ms). Any path that mixes packed-hours log fields with ms timestamps compares incompatible magnitudes. (Related to St-H6's hours-only TTL.) |
| CLN-11 | correctness / perf | `UtilizationProfile` is in-memory only; JE persists it to the `FileSummaryDB` and restores via `populateCache` at recovery. After a crash Noxu loses utilization detail and the skip-known-obsolete optimization. May be a deferred item — document if so. |
| CLN-12 | correctness (latent) | The periodic hook drains the look-ahead cache instead of calling `process_pending` (the code comments it as a future stub). Becomes a queue-growth bug once CLN-1 lands. |
| CLN-13 | perf | `do_clean` selects all files up front instead of refreshing the summary map between passes (JE refreshes so a just-cleaned file's effect on others is seen). |
| CLN-14 | perf | No `wakeupAfterNoWrites` → cleaned files may not be promptly deleted when write activity stops (deletion needs a checkpoint). |

### Safe-to-delete soundness (systemic)

JE's invariant: a file is deletable only after all its live LNs are migrated
**and** a checkpoint has flushed every dirty BIN (including user-database BINs)
so no slot points into the file. Noxu's checkpointer flushes only the internal
`primary_tree`, not user-database BINs (see `wave-gb-dbtree-recovery.md` and the
St-H6 Site-2 finding). Therefore the two-checkpoint barrier in `FileSelector`
buys *time* but not the *guarantee*: a user-database BIN can still hold a slot
pointing at a to-be-deleted file. **Confirming/fixing the checkpointer to flush
user-database BINs is a hard prerequisite for sound file deletion** and should
be treated as the gating issue for the whole cleaner-deletion path.

### JUSTIFIED (cleaner)

Single-threaded cleaner daemon (vs JE's N `FileProcessor` threads), no
embedded-LN / temporary-DB / deferred-write handling, no `OldBINDelta`
(pre-v8) compatibility, the EWMA throttle (a Noxu addition), and the
`Mutex<FileSelector>` synchronization model are all JUSTIFIED by `AGENTS.md`
scope decisions. MemoryBudget charging in `FileSelector` and the `dbIds` set
are documented porting gaps, not silent drift.

---

## C. Concurrency — latching, tree, evictor, daemons

### Faithful (verified)

Exclusive and shared latch semantics, BIN-latch-exclusive-only mode,
`LatchContext`, `release_if_owner`, and — importantly — the **hand-over-hand
latch coupling in the main descent paths** (`search`, `search_with_data`,
`first_entry_at_or_after`, `get_first_node`, `get_last_node`, `insert_recursive`)
via the `read_arc()` pattern, `split_root_if_needed`, `split_child` holding
`parent.write()` throughout, the evictor's `cursor_count` skip, and the daemon
condvar wake/shutdown mechanism.

### DRIFT (ranked)

| ID | Severity | Issue |
|---|---|---|
| CC-1 (D-2) | **critical — stale cursor / lost record** | No cursor-slot adjustment on BIN split. JE `BIN.adjustCursors(newSibling, low, high)` (`IN.java:4259`) rewires every cursor in the BIN's cursor set when a split moves its slot to the sibling or shifts its index. Noxu's `split_child` does nothing; `bin.rs` tracks a `cursor_set` but only `adjust_cursor_count` exists. A cursor on a BIN that splits can skip/revisit records silently. **Confirmed against source.** |
| CC-2 (D-3) | high — torn read | `first_entry_at_or_after_with_index` (`tree.rs` ~1942) uses check-then-lock (`arc.read().is_bin()` then a second `arc.read()`) instead of the coupled `read_arc()` pattern used everywhere else. A split in the gap can yield a false "not found" for an existing key (affects sorted-dup cursor search). |
| CC-3 (D-5) | high — shutdown data integrity | Daemon shutdown order is evictor→cleaner→checkpointer; JE requires **cleaner before checkpointer** (the cleaner calls the checkpointer) and stops the **evictor last** (so dirty nodes can still flush). Wrong order can drop final dirty-node writes on shutdown. |
| CC-4 (D-6) | medium — recovery correctness | Evictor always logs `Provisional::No`; JE's `coordinateEvictionWithCheckpoint` chooses `Provisional::Yes` for nodes evicted below the checkpoint's `maxFlushLevel`. An evict racing a checkpoint can produce a log the recovery cannot reconcile. |
| CC-5 (D-1) | medium — false fatal panic | `noxu-latch` `READ_HOLD_COUNT` is a single thread-local across *all* `SharedLatch` instances, so holding a read guard on latch L1 and acquiring a read guard on a *different* latch L2 panics ("already held in shared mode"). JE's reentrancy count is per-lock. Latent unless two independent `SharedLatch` reads are stacked. |
| CC-6 (D-4) | low–medium — liveness | Evictor uses blocking `node_arc.write()` in `strip_lns_from_node`/`flush_dirty_node_to_log`; under cursor read pressure the evictor can stall while the budget grows. JE checks `isPinned()` and uses non-blocking latch attempts. Fix: `try_write()` + put-back, and re-check `cursor_count` under the lock. |

### JUSTIFIED (concurrency)

`parking_lot` vs `ReentrantReadWriteLock` (non-reentrant; the reentrancy panics
are the compensating mechanism), reentrancy detected as `panic!` rather than
JE's environment-invalidating exception, BIN-latch-exclusive-only,
`split_child` holding the parent write latch (documented + reproducer test),
pre-emptive top-down splitting, and the `get_adjacent_bin` Arc-snapshot retry
(vs JE's parent re-search) are all JUSTIFIED.

---

## Recommended fix ordering

1. **CC-1 / cursor split adjustment** and the **cleaner safe-to-delete chain
   (CLN-1/2/3 + checkpointer-flushes-user-BINs)** are the correctness/data-loss
   items and should be fixed first. The checkpointer-user-BIN-flush work also
   unblocks T-F3/T-F4 and the cleaner deletion guarantee, and is the same
   blocker behind the deferred P-2 (see `wave-gb-dbtree-recovery.md`).
2. **CC-2, CC-3, TXN-2/3** next (torn read, shutdown integrity, txn bookkeeping).
3. **CC-4/CC-5/TXN-1/TXN-4/TXN-5** (recovery-race, latch-reentrancy, deadlock
   latency, dirty-read state check, handle-locker buddy).
4. The cleaner policy/perf items (CLN-4..14), CC-6, and TXN-6 — correctness-
   adjacent or performance; batch as a cleaner-fidelity pass.

Each fix should land with a regression test that fails before and passes after,
and should cite the JE source it aligns to.
