# Noxu DB — Second-Pass Cross-Feature Audit

**Date**: 2026-05-29  
**Auditors**: BDB-JE Sleepycat team, Margo Seltzer, Keith Bostic, Jon Gjengset (panel synthesis)  
**Branch**: `fix/wave11-r-semantic` (post-v2.4.2)  
**Scope**: Cross-feature and end-to-end interactions ONLY.  
  Per-subsystem findings from the 2026-05 first-pass audit (C-1..C-9, H-1..H-10, Q-1..Q-7) are NOT re-reported.

---

## Methodology

Each focus area below was investigated by tracing live code paths across crate
boundaries using `grep`, `find`, and `read` on the repository.  No build or
test execution was performed.  Code paths marked "unverified" require a
runtime test to confirm.

---

## Focus Area 1 — Replication × Recovery

### Finding X-1 (HIGH): VLSN Index Not Truncated After Replica Rollback Recovery

**Severity**: High  
**Interaction**: `noxu-rep::vlsn::VlsnIndex` × `noxu-recovery::RollbackTracker`  
**Files**:
- `crates/noxu-rep/src/vlsn/vlsn_index.rs` (`VlsnIndex::truncate_after`)
- `crates/noxu-recovery/src/rollback_tracker.rs` (`RollbackTracker`)
- `crates/noxu-rep/src/replicated_environment.rs:224–263` (VLSN index init)

**Gap**: When a replica crashes mid-syncup, recovery's `RollbackTracker` marks
log intervals covered by `RollbackStart..RollbackEnd` as invisible and skips
them during redo.  The recovery manager correctly calls
`rollback_tracker.register_rollback_start/end()`.  However, the in-memory
`VlsnIndex` is re-loaded from `vlsn.idx` on restart
(`crates/noxu-rep/src/replicated_environment.rs:231`) and is **never
consulted or truncated** by the recovery code path.  The VLSN index loaded
from disk may contain entries for VLSNs that were rolled back in the recovery
interval; `VlsnIndex::truncate_after()` exists but is not called from
recovery.

After crash-recovery the replica's VLSN index is out of sync with the
recovered B-tree: the index claims the latest committed VLSN is N but the
B-tree only reflects VLSNs ≤ M (where M < N due to rollback).  When the
feeder on the new master contacts this replica and queries
`vlsn_index.get_latest_vlsn()`, it starts streaming from the wrong position —
either re-sending already-applied entries (benign) or, if N is beyond the
master's current log tip, triggering an erroneous network restore.

**Repro sketch**:
1. Replica syncs to VLSN 1000.
2. Replica enters syncup with new master; master rolls back VLSNs 900–1000
   (partial network restore).
3. Replica crashes (kill -9) mid-rollback before recovery writes `RollbackEnd`.
4. Replica restarts; recovery marks VLSNs 900–1000 as rolled back but leaves
   `vlsn.idx` intact (loaded at step 4 with latest=1000).
5. Replica connects to master; claims it has VLSN 1000; master refuses because
   its log doesn't have 1000 anymore → unexpected network restore.

**Fix**: During recovery, after `rollback_tracker` is populated in the analysis
pass, call `vlsn_index.truncate_after(rollback_tracker.safe_high_watermark())`
before the replica reconnects.  Add a regression test that crashes a replica
mid-rollback and verifies the VLSN index is consistent with the recovered log.

---

### Finding X-2 (MEDIUM): VLSN Index Persistence Not Tied to Checkpoint Boundaries

**Severity**: Medium  
**Interaction**: `noxu-rep::vlsn::persist` × `noxu-recovery::checkpointer`  
**Files**:
- `crates/noxu-rep/src/replicated_environment.rs:467–535` (flush daemon)
- `crates/noxu-rep/src/vlsn/persist.rs:93` (`flush_to_disk`)
- `crates/noxu-recovery/src/checkpointer.rs` (checkpoint lifecycle)

**Gap**: `vlsn.idx` is flushed periodically in a background thread keyed on
time (every N seconds) with no coordination with the checkpointer.  The VLSN
index can advance past the last B-tree checkpoint: `vlsn.idx` records VLSNs up
to 5000 while the last durable checkpoint only covers VLSNs up to 4200.  After
a crash, the B-tree recovers to VLSN 4200 but the VLSN index claims 5000.
Recovery would not crash (the tree is internally consistent) but the stale high
watermark causes the replica to report a VLSN it doesn't actually have,
potentially leading to a feedgap mismatch when the master sends
`"you said you have 5000 but I need to feed from 4200"`.

**Repro sketch**: Write records to replica fast enough that the VLSN flush
fires between checkpoints; crash the replica; observe VLSN index > actual
committed tree state on restart.

**Fix**: Flush `vlsn.idx` only immediately after a successful checkpoint (or
flush both checkpoint and VLSN index atomically via the same fsync barrier).
Alternatively, on load, walk the log backward from the loaded
`get_latest_vlsn()` to the last `CkptEnd` and truncate the index.

---

## Focus Area 2 — XA × Recovery × Replication

### Finding X-3 (CRITICAL): Recovered XA Commit Written with NULL_VLSN — Invisible to Replication

**Severity**: Critical  
**Interaction**: `noxu-xa::environment::xa_commit` × `noxu-rep` VLSN tracking  
**Files**:
- `crates/noxu-db/src/environment.rs:1397–1455` (`write_txn_end_for_recovered`)
- `crates/noxu-db/src/environment.rs:1267–1305` (`apply_recovered_prepared_lns`)
- `crates/noxu-rep/src/vlsn/vlsn_index.rs` (`VlsnIndex::put`)

**Gap**: `write_txn_end_for_recovered()` writes the durable `TxnCommit` WAL
frame for a recovered prepared XA transaction with `NULL_VLSN`
(`crates/noxu-db/src/environment.rs:1414–1428`):

```rust
let entry = if is_commit {
    TxnEndEntry::new_commit(
        txn_id as i64,
        NULL_LSN,
        timestamp,
        0,
        NULL_VLSN,   // ← no VLSN assigned
    )
```

In a replicated environment, every committed transaction must carry a real VLSN
so the `VlsnIndex` can record the commit's log position and feeders can
stream it to replicas.  With `NULL_VLSN` the commit is invisible to the VLSN
tracker: replicas never learn about the resolved XA transaction, their VLSN
high-watermark stalls at the pre-crash level, and the master's own VLSN index
has no entry for these writes.

Additionally, `apply_recovered_prepared_lns` inserts into the tree using
`ln.original_lsn` — the LSN from the previous process — without writing new
LN WAL entries.  Feeders that start streaming after `xa_commit` resolution
will only see the `TxnCommit` record, not the LN data (which is already in the
old log files that replicas may or may not have).  Replicas that were
mid-syncup at crash time have a gap.

**Repro sketch**:
1. Open `XaEnvironment` over a `ReplicatedEnvironment` (master).
2. `xa_start` → insert 100 keys → `xa_end` → `xa_prepare` → crash master.
3. Restart master (becomes master again after election).
4. `xa_recover()` returns the XID.  `xa_commit(xid)`.
5. Connect a fresh replica.  Verify replica has the 100 keys.
6. Observe: VLSN tracker shows stale watermark; replica may not receive
   the 100 keys without a full network restore.

**Fix**: `write_txn_end_for_recovered` must allocate a new VLSN from the
environment's VLSN counter (same path as `Transaction::commit` in replicated
mode) and register it in the VLSN index.  Additionally, `apply_recovered_prepared_lns`
should write new LN WAL entries (not just update the tree in-memory) so feeders
can stream the resolved data.

---

### Finding X-4 (HIGH): Recovered XA Branch State Not Protected Against Concurrent xa_start

**Severity**: High  
**Interaction**: `noxu-xa::environment` `xa_start` × `recovered_branches` map  
**Files**:
- `crates/noxu-xa/src/environment.rs:241–264` (`xa_start`)
- `crates/noxu-xa/src/environment.rs:304–356` (`xa_commit` recovered path)

**Gap**: `xa_start` correctly rejects a duplicate XID if it exists in
`recovered_branches` (`lines 258–262`).  However, the `xa_commit` recovered
path drops the `recovered_branches` lock before calling
`apply_recovered_prepared_lns` and `write_txn_commit_for_recovered`
(`lines 339–358`).  A concurrent `xa_start` calling a JOIN on the same XID
during this window won't find it in `recovered_branches` (already removed) and
won't find it in `branches` (not yet inserted) — the `xa_start(JOIN)` returns
`XaError::NotFound` rather than `XaError::Protocol`, silently dropping a
valid branch join.

This is an edge case in the cross-process recovery protocol where the TM
calls `xa_start(JOIN)` immediately after the RM starts and `xa_commit` begins
resolving asynchronously.

**Repro sketch**: `xa_recover` in thread A, `xa_start(JOIN, xid)` in thread B
simultaneously; thread B sees `NotFound` instead of joining the recovery.

**Fix**: Hold the recovered_branches lock across the entire resolution phase,
or use a distinct in-progress-resolution state.

---

## Focus Area 3 — Cleaner × Checkpoint × Recovery

### Finding X-5 (CRITICAL): Checkpoint Barrier for Cleaned Files Is Implemented but Completely Disconnected

**Severity**: Critical  
**Interaction**: `noxu-cleaner::FileSelector` × `noxu-recovery::Checkpointer`  
**Files**:
- `crates/noxu-cleaner/src/file_selector.rs:359–455` (`mark_file_checkpointed`, `process_checkpoint_end`, `get_safe_to_delete`)
- `crates/noxu-cleaner/src/cleaner.rs:321–342` (`do_clean` and `delete_pending_files`)
- `crates/noxu-recovery/src/checkpointer.rs:416` (`do_checkpoint`)

**Gap**: `FileSelector` has a correct three-state checkpoint barrier:
`cleaned → checkpointed → safe_to_delete`.  The API methods
`process_checkpoint_end()`, `mark_file_checkpointed()`, and
`get_safe_to_delete()` are fully implemented and unit-tested in
`file_selector.rs`.

**These methods are never called from outside `noxu-cleaner`.**

```
grep -rn "process_checkpoint_end|mark_file_checkpointed|get_safe_to_delete" crates/
# Results: only crates/noxu-cleaner/src/file_selector.rs and its own tests.
```

The actual deletion path in `Cleaner::do_clean()` (`cleaner.rs:340`) is:

```rust
// Mark file as cleaned in selector
self.file_selector.lock().mark_file_cleaned(file_number);

// Mark file for deletion
self.pending_deletions.lock().push(file_number);
```

And `delete_pending_files()` (`cleaner.rs:731–750`) deletes files as soon as
`!self.file_protector.is_protected(file_number)` — which is true immediately
after active processing completes.  The file is deleted in the **same cleaning
pass**, without waiting for any checkpoint.

**Why this matters for Noxu's embedded-data model**: Unlike JE (where LN records
are separate from BINs and recovery must look up each slot's LSN to find the
data), Noxu embeds data inline in BINs.  This makes many paths safe after the
deletion — but NOT the undo path.

During recovery undo (`recovery_manager.rs:632`, `1491`):
```rust
// Non-embedded: read before-image from log.
let before_image = scanner.read_at_lsn(*abort_lsn);
```

If an uncommitted transaction's before-image LN lives in a log file that the
cleaner deleted (between the transaction's most recent write and the crash),
`scanner.read_at_lsn(*abort_lsn)` silently returns `None` and the recovery
undo path deletes the slot instead of restoring it.  This is silent data
corruption: the record is lost rather than reverted.

The path for corruption:
1. Transaction T updates key K (before-image at abort_lsn=5:100 in file 5).
2. Transaction T's next LN entry is in file 6 (after file 5).
3. Cleaner processes file 5, migrates active LNs, marks file 5 for deletion.
4. File 5 deleted (before any checkpoint — no barrier enforced).
5. T is still in flight (uncommitted).
6. Crash.
7. Recovery undo: tries `scanner.read_at_lsn(5:100)` → file 5 deleted → `None`.
8. Recovery `else { t.delete(&rec.key); }` — KEY K IS DELETED INSTEAD OF RESTORED.

**Repro sketch**:
```
env.open() with run_cleaner=true
db1 = env.open_database("primary")
txn = begin()
db1.put(&txn, "K", "before") // before-image at abort_lsn in file 5
db1.put(&txn, "K", "after")  // LN in file 6
// Let cleaner run and delete file 5 before txn commits
// kill -9 process
// Reopen: verify K == "before" (it will be MISSING)
```

**Fix**: Wire the checkpoint callback: after each `do_checkpoint()` succeeds,
call `cleaner.get_file_selector().lock().process_checkpoint_end(&state)` and
use `get_safe_to_delete()` (not `pending_deletions`) as the deletion source.
The checkpointer must expose the `CheckpointStartCleanerState` to the
environment, which forwards it to the cleaner at checkpoint completion.  This
is JE's `Checkpointer.hook(CleanerFileSelector.afterCheckpoint())` pattern.

---

### Finding X-6 (HIGH): Cleaner Migration Writes No WAL LN Entry — Fake LSN in Tree Slot

**Severity**: High  
**Interaction**: `noxu-cleaner::file_processor::SharedTreeLookup::migrate_ln_slot` × WAL  
**Files**:
- `crates/noxu-cleaner/src/file_processor.rs:686–720` (`SharedTreeLookup::migrate_ln_slot`)

**Gap**: The cleaner's shared-tree migration path does NOT write a WAL entry
for the migrated LN:

```rust
let new_lsn = self.log_manager.get_end_of_log();  // current end position
let _ = log_lsn;                                    // old LSN discarded

// ... (get data from tree)

let result = self.tree.read().map(|t| t.insert(key.to_vec(), data, new_lsn));
```

`get_end_of_log()` returns the current log head position; it is NOT a freshly
allocated and durably written LSN.  No LN log entry is written.  The slot is
only marked dirty (via `insert`), which defers durability to the next
checkpoint or evictor flush.

If a crash occurs between migration and the next checkpoint:
- Recovery loads the BIN from the last checkpoint; the slot shows the
  pre-migration LSN pointing to the old (potentially deleted) file.
- Recovery's LN redo finds no "migration" WAL entry.
- With the X-5 checkpoint-barrier gap, the old file may be gone.

Even without X-5, the fake `new_lsn` means the slot's LSN-ordering is wrong:
a user transaction that updates key K between migration and crash has a real
WAL LSN.  Recovery's redo ordering (`logrecLsn > treeLsn → replace`) may
apply that update against the pre-migration LSN, potentially producing a
spurious redo.

The `RealTreeLookup::migrate_ln_slot` path (line 295) has the same problem:
it uses `log_lsn` (the OLD file's LSN) as the new slot LSN, not a freshly
logged entry.

**Fix**: `migrate_ln_slot` must write a non-transactional LN WAL entry (via
`lm.log(LogEntryType::LN, &buf, ...)`) and use its returned LSN for the
in-memory slot update.  This is how JE's `LN.log(locker, ...)` works in the
migration path.

---

## Focus Area 4 — Secondary Indexes × Transactions × Cleaner

### Finding X-7 (MEDIUM): Cleaner Migration Does Not Distinguish Secondary Databases

**Severity**: Medium  
**Interaction**: `noxu-cleaner::file_processor` × `noxu-db::secondary_database`  
**Files**:
- `crates/noxu-cleaner/src/file_processor.rs:184–300` (`migrate_ln_slot`)
- `crates/noxu-db/src/secondary_database.rs:4–16` (data model doc comment)

**Gap**: `migrate_ln_slot` uses a single `SharedTreeLookup` backed by the
primary tree.  When the cleaner processes a secondary database's log file, it
looks up the key in the primary tree to check if the LN is still live.

A secondary database stores `sec_key → pri_key` pairs.  The primary tree
lookup for a `sec_key` will naturally return `NotFound` (secondary keys are not
in the primary tree), causing every secondary LN to be classified as
`MigrationOutcome::Obsolete`.  Secondary LNs in files being cleaned are
effectively "not migrated" — they are left unreferenced.

In practice, because secondary databases have their own `DatabaseImpl` with a
separate tree, and the cleaner is currently only wired to the primary tree
(`environment_impl.rs:462–502`), the cleaner processes the primary tree's log
files but may not correctly handle secondary databases' files.

**Repro sketch**:
1. Create a primary DB and a secondary DB.
2. Insert 10000 records.
3. Run the cleaner on a log file that has secondary LN entries.
4. Verify secondary entries are still accessible after cleaning.

**Suggested test**: unverified — needs a runtime cleaner test with secondary
databases wired.

**Note**: Secondary databases store `sec_key → pri_key`, NOT `sec_key → LSN`,
so secondary entries are not invalidated by cleaner migration of primary LNs.
This is architecturally correct.  The concern above about liveness
classification is the primary risk here.

---

## Focus Area 5 — Eviction × Dirty BIN × Checkpoint

### Finding X-8 (MEDIUM): Evictor + Checkpointer Race Writes Redundant Empty BINDelta

**Severity**: Medium  
**Interaction**: `noxu-evictor::Evictor::flush_dirty_node_to_log` × `noxu-recovery::Checkpointer::flush_dirty_bins_internal`  
**Files**:
- `crates/noxu-evictor/src/evictor.rs:591–637` (`flush_dirty_node_to_log`)
- `crates/noxu-recovery/src/checkpointer.rs:598–705` (`flush_dirty_bins_internal`)

**Gap**: Both the evictor and the checkpointer flush dirty BINs to the WAL
under a per-node write lock.  They cannot simultaneously hold the same node's
write lock, so the data written is always consistent.  However:

1. Checkpointer's `flush_dirty_bins_internal()` calls `collect_dirty_bins()`
   under a **tree read lock** to build the dirty-BIN list (snapshotted Arcs).
2. Before the checkpointer acquires the **node write lock** for a specific BIN,
   the evictor may have already flushed and cleared it (`dirty = false`,
   `last_full_lsn = X`).
3. The checkpointer then acquires the write lock and checks:
   ```rust
   if total == 0 && !b.dirty { continue; }  // skips ONLY empty-AND-clean nodes
   ```
   A node with `total > 0` and `!b.dirty` (cleaned by evictor) is NOT skipped.
4. With `dirty_count = 0` and `last_full_lsn != NULL_LSN`, `use_delta = true`
   (`0/total = 0.0 ≤ 0.25 = TREE_BIN_DELTA`).
5. Checkpointer writes an **empty BINDelta** (0 dirty entries) for a BIN that
   the evictor already flushed.  This is a no-op entry but wastes log space and
   updates `b.last_delta_lsn` unnecessarily.

The correct fix is simple:  add `if dirty_count == 0 && !b.dirty { continue; }`
after the current guard at the top of the per-BIN loop.

**[overlaps H-9 indirectly]**: H-9 (PartialEvict fix) strips LNs but leaves
dirty slots; the evictor-vs-checkpointer race applies to the same BIN paths.
H-9 does not introduce new cross-feature issues here since `strip_lns` skips
dirty slots.

**Fix**: Add `if !b.dirty && b.dirty_count() == 0 { continue; }` before the
delta-vs-full decision in `flush_dirty_bins_internal`.

---

## Focus Area 6 — Cursor × Eviction × Concurrent Modification

### Finding X-9 (LOW): strip_lns Cursor-Count Check Is Correct — No New Issue Found

**Severity**: Low (confirmed correct)  
**Files**: `crates/noxu-tree/src/tree.rs:313–328` (`BinStub::strip_lns`)

`strip_lns` opens with:
```rust
if self.cursor_count > 0 {
    return 0;
}
```
This correctly protects any cursor positioned on the BIN.  The `cursor_count`
is incremented by `Tree::pin_bin()` (cursor acquisition) and decremented by
`Tree::unpin_bin()`.  The evictor's `strip_lns_from_node()` acquires a write
lock on the node and calls `bin.strip_lns()`, which re-reads `cursor_count`
under that write lock — the same lock that `Tree::pin_bin()` must acquire to
increment the count.

**Cross-BIN cursor boundary during eviction**: When a cursor is mid-`get_next`
at a BIN boundary, it:
1. Releases the old BIN's write lock (via `update_bin_pin`).
2. Calls `Tree::get_next_bin()`, traverses the tree to locate the sibling.
3. Re-pins the new BIN.

Between steps 1 and 3 the old BIN's `cursor_count = 0` and the new BIN is not
yet pinned.  The evictor *could* start evicting the new (right-sibling) BIN
during this window.  Because `strip_lns` only clears non-dirty data and
`cursor_count` is checked at the moment of strip, this is safe: the cursor
will re-pin the sibling and the evictor will see `cursor_count > 0` after the
pin, preventing further stripping.

No new issue — this area is sound.

---

## Focus Area 7 — Transaction Abort × Secondary Indexes × Cursors

### Finding X-10 (HIGH): Secondary Index Update Under Txn Abort — Cursor on Secondary May See Torn State

**Severity**: High  
**Interaction**: `noxu-db::Database::put` secondary hooks × `noxu-db::SecondaryCursor`  
**Files**:
- `crates/noxu-db/src/database.rs:766–790` (secondary hook fanout under txn)
- `crates/noxu-db/src/secondary_database.rs:160–260` (`update_secondary`)
- `crates/noxu-db/src/secondary_cursor.rs:72–79` (auto-commit note)

**Gap**: When `Database::put(&txn, key, new_value)` runs:
1. The primary record is written under `txn`.
2. The secondary hook loop (`database.rs:766–790`) calls each secondary's
   `update_secondary(&txn, old_key, new_key, pri_key)`, which calls
   `delete_sec_key(&txn, old_sec_key, pri_key)` then
   `insert_sec_key(&txn, new_sec_key, pri_key)`.

All writes are under the same `txn`.  When `txn.abort()` is called, the undo
loop must revert both the primary LN and each secondary LN in reverse order.

The gap: a **concurrent reader** holding a `SecondaryCursor` positioned on
`old_sec_key` in the secondary database is NOT protected by any lock during
the abort's undo pass.  The undo loop acquires write locks per key slot
individually in reverse order.  Between the undo of the secondary delete
(restoring `old_sec_key`) and the undo of the primary write (restoring the
old primary value), a snapshot window exists where:

- Secondary cursor sees `old_sec_key → pri_key` (restored).
- Primary read for `pri_key` yields the NEW value (not yet reverted).

This is a classic read-isolation torn-state window in the abort path.  It is
**not** blocked by any secondary-database-level lock in the current code.

The `secondary_cursor.rs:72–79` comment acknowledges prior issues with
auto-commit secondary cursors; the concurrent-abort case is separate.

**[overlaps C-8 area]**: C-8 (SR9465/SR9752) covers rollback of duplicate
inserts within a single txn.  This finding concerns CROSS-txn visibility
during abort's multi-step undo.

**Repro sketch**:
```
txn1 = begin()
db_primary.put(&txn1, "K", "V_new")  // was "V_old"
// secondary key changes from sec("V_old") to sec("V_new")
// Thread 2 immediately opens secondary cursor on sec("V_new")
txn1.abort()  // undo secondary, then primary
// Thread 2 cursor on secondary sees old sec key → new primary value momentarily
```

**Fix**: The abort undo pass must hold a write lock on the secondary database
for the duration of the undo, or use a secondary-level latch.  Alternatively,
the secondary cursor should re-verify the primary record under a lock after
traversal.

---

## Focus Area 8 — Config Consistency

### Finding X-11 (HIGH): `log_flush_no_sync_interval_ms` Config Parameter Silently Ignored

**Severity**: High  
**Interaction**: `EnvironmentConfig` → `DbiConfig` → **nowhere**  
**Files**:
- `crates/noxu-db/src/environment_config.rs:272–273` (field definition)
- `crates/noxu-db/src/environment.rs:262` (passed to DbiConfig)
- `crates/noxu-dbi/src/dbi_config.rs:64` (stored)
- `crates/noxu-dbi/src/environment_impl.rs` (not consumed anywhere)

**Gap**: `EnvironmentConfig::log_flush_no_sync_interval_ms` is documented as
"controls background log flush interval when using COMMIT_NO_SYNC."  It flows:
`EnvironmentConfig → DbiConfig` (line 262) and is stored.  It is **never read
or acted upon** by the `EnvironmentImpl` daemon startup, the `LogManager`, or
any background thread.

A user who sets:
```rust
config.set_log_flush_no_sync_interval_ms(100);
```
and uses `CommitNoSync` transactions gets NO background flush; data remains in
application write buffers indefinitely.  The documented `LogFlushTask` daemon
(Q-3 from first pass) is the missing consumer.  The cross-feature aspect is
that the config value is silently propagated through multiple layers without
anyone noticing the dead-end.

**[overlaps Q-3]**: Q-3 flags `LogFlushTask` as a missing feature.  This
finding specifically flags the dead config propagation path as a separate
cross-layer issue.

**Fix**: Either wire the interval to a background flush daemon, or document
the parameter as `#[doc = "NOT YET IMPLEMENTED — use CommitSync or
CommitWriteNoSync for durability guarantees."]` and return an error if
non-zero.

---

### Finding X-12 (HIGH): `lock_timeout_ms` Propagation Is Correct; `cache_size` Budget Is Fractured

**Severity**: High (budget fracture); Informational (lock_timeout)  
**Interaction**: `EnvironmentConfig` cache budget → `Arbiter` × `LogManager` × `OffHeapCache`  
**Files**:
- `crates/noxu-dbi/src/environment_impl.rs:484–502` (budget allocation)
- `crates/noxu-evictor/src/arbiter.rs:38–56` (`Arbiter`)

**`lock_timeout_ms` — CORRECT**: `EnvironmentImpl::new()` at line 288 calls
`lock_manager.set_lock_timeout(cfg.lock_timeout_ms)`.  Dynamic updates also
flow correctly via `environment.rs:966–970`.  No issue.

**`cache_size` budget — FRACTURED**: The `Arbiter` (BIN tree budget) is
created with `cache_bytes = cfg.cache_size` only:

```rust
let cache_bytes = cfg.cache_size as i64;
let arbiter = Arbiter::new(cache_bytes, ...);
```

The `LogManager` independently allocates `cfg.log_buffer_size` bytes.
The `OffHeapCache` independently allows `cfg.max_off_heap_memory` bytes.

The three pools are:
- BIN tree heap: bounded by `cache_size` via Arbiter
- Log write buffers: bounded by `log_buffer_size` (separate pool)
- Off-heap BIN store: bounded by `max_off_heap_memory` (separate pool)

Total actual memory usage = `cache_size + log_buffer_size + max_off_heap_memory`.
A user who sets `cache_size = 256 MiB` expecting 256 MiB total may actually
use 256 + 64 + 512 = 832 MiB if the other params are non-zero defaults.
There is no combined budget enforcer.

**[overlaps implicitly with H-3]**: H-3 is about per-entry allocation overhead.
X-12 is about the budget ceiling not being collectively enforced.

**Fix**: Document the additive budget model clearly.  Optionally create a
shared `MemoryBudget` struct that aggregates all three pools; the Arbiter reads
from it rather than maintaining an independent counter.

---

## Focus Area 9 — Error Propagation End-to-End

### Finding X-13 (HIGH): Database and Cursor Operations Do Not Check Environment Validity

**Severity**: High  
**Interaction**: `noxu-dbi::EnvironmentImpl::is_valid()` × `noxu-db::Database::check_open()` × `noxu-dbi::CursorImpl::check_state()`  
**Files**:
- `crates/noxu-dbi/src/environment_impl.rs:752–765` (`is_valid`)
- `crates/noxu-db/src/database.rs:1470–1476` (`check_open`)
- `crates/noxu-dbi/src/cursor_impl.rs:646–655` (`check_state`)
- `crates/noxu-log/src/log_manager.rs:273` (io_invalid guard on write)

**Gap**: After C-2's fix (env invalidation on fsync failure) set
`io_invalid = true` and made `EnvironmentImpl::is_valid()` return `false`,
the invalidation propagates to **writes** correctly (LogManager's `log()`
checks `io_invalid` at entry).  However, **reads** and **cursor operations**
bypass the validity check entirely.

`Database::check_open()` (`database.rs:1470`):
```rust
fn check_open(&self) -> Result<()> {
    if !self.open.load(Ordering::Acquire) {
        return Err(NoxuError::DatabaseClosed);
    }
    Ok(())  // ← env validity NOT checked
}
```

`CursorImpl::check_state()` (`cursor_impl.rs:646`):
```rust
fn check_state(&self) -> Result<(), DbiError> {
    match self.state {
        CursorState::Closed => Err(DbiError::CursorClosed),
        _ => Ok(()),   // ← env validity NOT checked
    }
}
```

After an fsync failure:
- `db.get()`, `db.put()` (read path), `cursor.get_next()` all succeed at the
  check-open level.
- Only the `lm.log()` call inside the write path returns an error.
- Read-only operations on an invalidated environment continue silently.

A replication feeder (`crates/noxu-rep/src/stream/feeder.rs`) has **no
env-validity check** at all — it continues streaming from a log that may be
corrupt because `io_invalid` was set after an fdatasync failure.

**Repro sketch**:
1. Open environment with WAL.
2. Inject `io_invalid = true` (or trigger an fdatasync failure).
3. Verify `env.is_valid()` returns `false`.
4. Call `db.get(None, &key, &mut val)` — succeeds (reads stale BIN data).
5. Call `db.put(None, &key, &val)` — fails at `lm.log()` but NOT at `check_open()`.
6. The caller's error handling sees two different error origins for reads
   vs. writes, making it hard to fence all access uniformly.

**Fix**: `Database::check_open()` and `CursorImpl::check_state()` must call
`env_impl.check_open()` (which checks `is_valid()`).  The cached
`log_manager.io_invalid` Arc can be stored in Database and Cursor at open
time so the hot path does not acquire `env_impl.lock()`.  Add a
`check_env_validity()` helper that loads the AtomicBool.

---

## Focus Area 10 — End-to-End Scenario Walk

### Finding X-14 (HIGH): Env Open → Primary + Secondary Txn → Checkpoint → Crash → Recover — VLSN Not Rebuilt

**Severity**: High  
**Interaction**: `noxu-recovery` × `noxu-rep` VLSN rebuild on recovery  
**Files**:
- `crates/noxu-rep/src/replicated_environment.rs:224–263` (VLSN index init)
- `crates/noxu-recovery/src/recovery_manager.rs:432–540` (analysis pass)

**Gap**: When a `ReplicatedEnvironment` opens after a crash:
1. `vlsn.idx` is loaded from disk.
2. `RecoveryManager::recover_all()` is called on the B-tree.
3. The recovery analysis pass processes `LogEntry::Ln` records but does NOT
   populate or update the VLSN index.

The result: after crash recovery on a replica, the in-memory VLSN index is the
stale snapshot from `vlsn.idx` (which may be newer or older than the actual
recovered log position), NOT a freshly rebuilt index consistent with the
recovered B-tree.

JE's `RepImpl.buildTree()` rebuilds the VLSN index as part of recovery by
re-scanning VLSN entries from the log.  Noxu has no equivalent.

**Repro sketch**:
1. Replica at VLSN 500 (vlsn.idx has latest=500).
2. Replica receives VLSNs 501–600 but crashes before persisting vlsn.idx
   (index is still at 500 on disk).
3. Restart: recovery replays VLSNs 501–600 into the B-tree.
4. VLSN index shows latest=500 but B-tree has data from 501–600.
5. Feeder reconnects at VLSN 501 and re-sends all of 501–600 — harmless but
   wasteful.  Or, if vlsn.idx was flushed at 600 but recovery only reaches 550
   (X-2), the feeder skips 551–600 entirely.

**Fix**: After `recover_all()` completes, rebuild the VLSN index by scanning
the recovered LN entries that carry `vlsn != NULL_VLSN`.  This can be done
cheaply in a second pass over `redo_entries` collected during analysis.

---

### Finding X-15 (CRITICAL): Replica Sync-Up → Master Failover During Syncup — Matchpoint LSN Not Validated Against Recovered Log

**Severity**: Critical (unverified — needs runtime test)  
**Interaction**: `noxu-rep::stream::replica_stream` × `noxu-recovery::RollbackTracker`  
**Files**:
- `crates/noxu-rep/src/stream/replica_stream.rs:173–196` (stream recovery)
- `crates/noxu-recovery/src/rollback_tracker.rs`

**Gap**: When the master fails over mid-syncup, a replica that was receiving a
rollback stream is left in a partially rolled-back state.  On restart, the
`RollbackTracker` is populated from `RollbackStart`/`RollbackEnd` markers in
the log.  However, if the replica crashes AFTER writing some rolled-back
entries but BEFORE `RollbackEnd` is written, the `RollbackTracker` has a
`RollbackStart` with no matching `RollbackEnd`.

In `recovery_manager.rs`:
```rust
LogEntry::RollbackStart(rec) => {
    self.rollback_tracker.register_rollback_start(rec.matchpoint_lsn, rec.lsn);
}
LogEntry::RollbackEnd(rec) => {
    self.rollback_tracker.register_rollback_end(rec.matchpoint_lsn, rec.lsn);
}
```

The recovery redo phase calls `rollback_tracker.is_in_rollback_period(lsn)`
for each entry.  If no `RollbackEnd` exists, is the entire tail of the log
treated as a rollback period?  The behavior of
`RollbackTracker::is_in_rollback_period` for an open-ended rollback interval
is **unverified** from static analysis alone.

**Repro sketch**:
1. Replica receives VLSNs 1–1000.  Master fails.
2. New master triggers syncup; sends `RollbackStart(matchpoint=900)`.
3. Replica writes `RollbackStart` to its log, starts rolling back.
4. Replica crashes at VLSN 950 (during rollback, before `RollbackEnd`).
5. Restart recovery: is `is_in_rollback_period(950..1000)` correctly handled?

**Suggested test**: Add a test that injects a `RollbackStart` without a
matching `RollbackEnd` and verifies recovery correctly handles entries in the
open-ended rollback interval.

---

## Focus Area 11 — Stateright Specs vs Cross-Feature Reality

### Finding X-16: Spec Coverage Gaps in Cross-Feature Interactions

The 11 existing Stateright specs cover:
- `btree_latching` — single-crate, no cross-feature  
- `cache_vs_cleaner` — evictor × cleaner dirty-bit ordering (in-process race)  
- `cleaner_safety` — reader refs preventing file deletion (NOT the checkpoint barrier)  
- `flexible_paxos`, `vlsn_streaming`, `master_transfer`, `network_restore` — replication protocols  
- `recovery_three_phase` — analysis → redo → undo (single-threaded, no XA, no cleaner)  
- `wal_commit` — log group-commit  
- `xa_two_phase_commit` — XA state machine (single RM, no replication)  

**Gaps confirmed by this audit**:

1. **`cleaner_safety` does NOT model the checkpoint barrier** (X-5).  It models
   "reader refs protect files from deletion" but NOT "a file must survive until
   a checkpoint captures the migrated entries."  The existing spec will not
   catch the X-5 bug.

2. **No spec for XA × Recovery × Replication** (X-3).  The `xa_two_phase_commit`
   spec models a single-process XA commit.  There is no model covering "prepared
   XA survives master crash, recovered on restart, committed with VLSN
   assignment, replicated to followers."

3. **No spec for VLSN index × Recovery consistency** (X-1, X-14).  The
   `vlsn_streaming` spec models the feeder/replica streaming protocol but not
   the post-crash VLSN rebuild invariant.

4. **No spec for env-invalidity propagation** (X-13).  No model ensures that
   after `io_invalid = true`, all public API operations return an error.

---

## Summary Table

| ID  | Severity | Focus Area | Interaction | Overlaps 1st-pass? |
|-----|----------|------------|-------------|--------------------|
| X-1  | High     | Rep × Recovery | VLSN index not truncated after replica rollback recovery | No |
| X-2  | Medium   | Rep × Recovery | VLSN persist not tied to checkpoint | No |
| X-3  | **Critical** | XA × Rep | Recovered XA commit written with NULL_VLSN | No |
| X-4  | High     | XA × Rep | Recovered branch TOCTOU in xa_commit | No |
| X-5  | **Critical** | Cleaner × Ckpt × Recovery | Checkpoint barrier disconnected; before-image undo can read deleted file | No |
| X-6  | High     | Cleaner × WAL | Migration writes no WAL entry; fake LSN in tree slot | No |
| X-7  | Medium   | Cleaner × Secondary | Cleaner uses primary lookup for secondary LN liveness | No |
| X-8  | Medium   | Evictor × Ckpt | Redundant empty BINDelta written after evictor flushes | No |
| X-9  | Low      | Cursor × Evictor | strip_lns cursor-count check is correct | No (confirmed sound) |
| X-10 | High     | Txn Abort × Secondary × Cursor | Torn-state window during secondary index abort undo | Partially overlaps C-8 |
| X-11 | High     | Config × WAL | log_flush_no_sync_interval_ms silently ignored | Overlaps Q-3 (new cross-layer aspect) |
| X-12 | High     | Config × Memory | cache_size budget fractured across 3 independent pools | No |
| X-13 | High     | Error prop | DB/cursor check_open bypasses env validity check | No |
| X-14 | High     | Recovery × Rep | VLSN not rebuilt during recovery | No |
| X-15 | Critical | Rep × Recovery | Matchpoint LSN in open-ended rollback unverified | No |
| X-16 | n/a      | Stateright | Spec coverage gaps in 3 cross-feature areas | No |

---

## Genuinely New (Not in First-Pass Reports)

The following findings are NOT covered by C-1..C-9, H-1..H-10, or Q-1..Q-7:

1. **X-3** (Critical): Recovered XA commit uses NULL_VLSN — silent VLSN gap in replication.
2. **X-5** (Critical): Cleaner checkpoint barrier fully implemented in `FileSelector` but never called from any other component; before-image undo can fail on deleted files.
3. **X-15** (Critical, unverified): Replica mid-rollback crash leaves open-ended `RollbackStart` with unknown recovery behavior.
4. **X-1** (High): VLSN index not truncated to match B-tree rollback on replica restart.
5. **X-6** (High): Cleaner migration writes no WAL entry and inserts a fake LSN into the tree slot.
6. **X-4** (High): Recovered XA branch TOCTOU window in `xa_commit` resolution.
7. **X-10** (High): Secondary index abort undo has cross-cursor torn-state window.
8. **X-12** (High): Memory budget is fractured — cache_size + log_buffer + off_heap are three independent ceilings.
9. **X-13** (High): `Database::check_open()` and `CursorImpl::check_state()` do not check `env.is_valid()`.
10. **X-14** (High): VLSN index not rebuilt during B-tree recovery.
11. **X-8** (Medium): Redundant empty BINDelta when evictor races checkpointer.
12. **X-11** (High): `log_flush_no_sync_interval_ms` flows to `DbiConfig` but is never consumed.
13. **X-2** (Medium): `vlsn.idx` persistence not tied to checkpoint boundaries.
14. **X-7** (Medium): Cleaner uses primary tree for secondary LN liveness check.

**Partially overlaps first-pass**:
- X-10 partially overlaps C-8 (SR9465/SR9752 area — single-txn rollback) but the cross-cursor torn-state during abort is a new dimension.
- X-11 overlaps Q-3 (LogFlushTask missing) but the dead config propagation path through multiple crates is a new cross-layer observation.

---

## Recommended New Stateright Specs

1. **`cleaner_checkpoint_barrier`**: Model the three-state file lifecycle
   (`cleaned → checkpointed → safe_to_delete`) and the invariant that
   `Delete(file)` can only fire after the checkpointer fires
   `process_checkpoint_end` for that file.  The current `cleaner_safety` spec
   models reader-ref liveness but not checkpoint ordering.

2. **`xa_recovery_replication`**: Model a single master XA transaction that
   survives crash-and-recovery, with VLSN assignment on resolution and
   replication to N replicas.  Invariant: after `xa_commit` the master's VLSN
   index has an entry ≥ the last VLSN before crash, and all replicas
   eventually converge.

3. **`vlsn_recovery_consistency`**: Model the invariant that after crash
   recovery, `vlsn_index.get_latest_vlsn() ≤ last_committed_lsn_in_log`.
   The index must be truncated to the recovered log position.

4. **`env_invalidity_propagation`**: Model `EnvironmentImpl::invalidate()` as
   a state transition and assert that every subsequent public operation
   (read, write, cursor, commit, abort) returns `EnvironmentFailure`.

---

## Top 5 Most-Actionable New Findings

### 1. X-5 — Cleaner Checkpoint Barrier Disconnected (Critical)
**Why top**: Silent data corruption in the undo recovery path when before-image
LNs live in a deleted log file.  The checkpoint barrier infrastructure exists
in `FileSelector` (it just isn't called).  Fix requires wiring
`process_checkpoint_end` from the checkpointer to the cleaner — roughly 10
lines of plumbing.

**Immediate action**:
- Add `assert!(!process_checkpoint_end_was_called)` CI instrumentation to
  confirm nobody is calling these methods.
- Wire `Cleaner::process_checkpoint_end(state)` from
  `Checkpointer::do_checkpoint()` at step 6.
- Change `delete_pending_files()` to use `file_selector.get_safe_to_delete()`
  as the authoritative deletion list, not `pending_deletions`.

### 2. X-3 — Recovered XA Commit Uses NULL_VLSN (Critical)
**Why top**: A prepared XA transaction that survives a master crash and is
committed via `xa_commit` on restart is invisible to replicas' VLSN tracking.
Replicas never learn the commit happened; the committed data is accessible
locally but unreplicated.

**Immediate action**: In `write_txn_end_for_recovered`, allocate a VLSN from
the environment's VLSN generator (same codepath as a normal commit in
replicated mode) and register it via `vlsn_index.put(vlsn, lsn)`.

### 3. X-13 — DB/Cursor check_open Bypasses Env Validity (High)
**Why top**: After the C-2 fix (env invalidation on fsync failure), reads can
continue on a potentially corrupt environment.  This is a single-line fix per
check function, and the `io_invalid` AtomicBool is already cached in
`LogManager` which `Database` holds.

**Immediate action**: In `Database::check_open()`:
```rust
fn check_open(&self) -> Result<()> {
    if self.log_manager.as_ref()
        .map_or(false, |lm| lm.io_invalid.load(Ordering::Acquire))
    {
        return Err(NoxuError::EnvironmentInvalid("I/O failure".into()));
    }
    if !self.open.load(Ordering::Acquire) {
        return Err(NoxuError::DatabaseClosed);
    }
    Ok(())
}
```

### 4. X-6 — Cleaner Migration Writes No WAL Entry (High)
**Why top**: Directly amplifies X-5.  Even with X-5 fixed (checkpoint barrier),
if the migration doesn't write a WAL LN entry, recovery after crash-before-
checkpoint cannot find the migrated data in the WAL.  The BIN dirty flag is the
only bridge to durability, and that bridge requires a checkpoint to survive.
Fix: write a non-transactional LN WAL entry in `migrate_ln_slot` before
updating the tree slot.

### 5. X-1 — VLSN Index Not Truncated After Rollback Recovery (High)
**Why top**: After a replica crashes mid-rollback, the VLSN index is inconsistent
with the recovered B-tree state.  The feeder reconnects at the wrong VLSN,
causing unnecessary re-syncs or silent data gaps.  `VlsnIndex::truncate_after()`
exists; it just needs to be called from the recovery path after
`rollback_tracker` is populated.

---

## Counts of Genuinely New Findings by Severity

| Severity | Count | IDs |
|----------|-------|-----|
| Critical | 3 | X-3, X-5, X-15 (unverified) |
| High     | 8 | X-1, X-4, X-6, X-10, X-11, X-12, X-13, X-14 |
| Medium   | 3 | X-2, X-7, X-8 |
| Low      | 1 | X-9 (confirmed sound, no issue) |

**Total genuinely new cross-feature findings**: 14 (plus 1 unverified critical).  
**Confirmed safe**: X-9 (strip_lns cursor-count check is correct).

---

*Report generated by static analysis only; no builds or tests were executed.*  
*All code citations reference branch `fix/wave11-r-semantic` as of 2026-05-29.*
