# Noxu DB — Production-Readiness Review: Transactions, Isolation, Locking, Recovery

**Code base verified against**: `/tmp/noxu-review` (origin/main, v3.2.0,
commit `34171f6`).  The stale branch `fix/zb-stale-docs` was used only for
initial orientation; every file:line citation below was re-checked on main
before inclusion.  Two findings from the draft pass were dropped after main
showed them already fixed (env.close() active-txn tracking, analysis-result
phantom-active-txn guard).

**Reviewer perspective**: Michael Cahill (serializable isolation / WiredTiger)
· Justin Sheehy (distributed-systems correctness / ops) · Charlie Lamb
(BDB-JE transactions)

**Scope**: `noxu-txn` (txn.rs, txn_manager.rs, lock_manager.rs,
deadlock_detector.rs), `noxu-recovery` (recovery_manager.rs,
checkpointer.rs, analysis_result.rs), isolation surface in `noxu-dbi`
(cursor_impl.rs) and `noxu-db` (transaction.rs, environment.rs).

**JE reference**: `/home/gburd/ws/je/src/com/sleepycat/je/`

---

## Summary Table

| ID  | Sev      | Component                          | Title                                                                  |
|-----|----------|------------------------------------|------------------------------------------------------------------------|
| F-1 | Critical | recovery_manager.rs                | Undo pass lacks LSN currency check — committed writes silently lost    |
| F-2 | Critical | cursor_impl.rs + isolation docs    | SERIALIZABLE range locks never acquired; phantoms not prevented        |
| F-3 | High     | checkpointer.rs                    | `first_active_lsn = Lsn::new(0,0)` in CkptEnd; O(log) recovery always |
| F-4 | High     | txn_manager.rs / cursor_impl.rs    | `update_first_lsn` defined but never called; firstActiveLsn always NULL|
| F-5 | High     | txn_manager.rs / transaction.rs    | `TxnManager::commit_txn`/`abort_txn` not called for explicit txns      |
| F-6 | Medium   | txn.rs                             | `retains_locks_on_commit()` returns `true` but commit drains unconditionally |
| F-7 | Medium   | lock_manager.rs                    | `lock_with_sharing_and_timeout` skips per-wakeup deadlock re-check     |
| F-8 | Medium   | isolation.md / durability.md       | Doc claims "range locks" for SERIALIZABLE; code delivers plain reads   |
| F-9 | Medium   | prop_tests.rs / analysis_result.rs | Stale TODO claims open bug; defensive guard is already present in code |
| F-10| Low      | txn.rs                             | `lock_after_lsn_change`: dead code with both error paths silently swallowed |

**Total**: 2 Critical · 3 High · 4 Medium · 1 Low = **10 findings**

---

## F-1 — Critical: Recovery Undo Pass Lacks LSN Currency Check — Committed Writes Silently Lost

### Files:lines (main)
`crates/noxu-recovery/src/recovery_manager.rs:789,1643` (`compute_undo_action` call
sites in `run_undo_all` and `run_undo`).
`crates/noxu-recovery/src/recovery_manager.rs:1773–1776` (code comment claiming the
check is delegated to the tree layer).
`crates/noxu-tree/src/tree.rs:561,646` (`insert_with_prefix` / `insert_with_prefix_slice`
unconditional overwrite paths).

### What JE does
`RecoveryManager.java:1817–1832` documents the complete decision table.  The
implementation at line 1949 enforces row 3 ("logLsn ≠ slotLsn → no action"):

```java
long slotLsn = location.childLsn;          // current LSN in the BIN slot
boolean updateEntry = DbLsn.compareTo(logLsn, slotLsn) == 0;
if (updateEntry) {
    bin.recoverRecord(...);  // only revert when slot still holds THIS version
}
```

### What Noxu does
The doc comment at line 1773 reads:

> "The `logLsn == slotLsn` currency checks are **delegated to the tree
> layer** (`Tree::delete` / `Tree::insert`) at the call site."

This claim is false.  `Tree::insert_with_prefix_slice` (tree.rs:646) and
`Tree::insert_with_prefix` (tree.rs:561) unconditionally overwrite the slot
LSN and data for any key that exists — there is no prior-version guard.
`Tree::delete` removes the key without checking the current slot LSN.
Neither method has a `get_slot_lsn` call, and no such method exists on main.

*(Note: the `fix/gb-dbtree-recovery` branch prototyped an LSN-aware
`redo_insert` but that branch was explicitly not merged to main;
`wave-gb-dbtree-recovery.md:280` describes it as unmerged.)*

### Concrete data-loss scenario

```
T1 writes key K at LSN 200 (uncommitted).
T1 aborts (TxnAbort at LSN 250); T1 held write lock → releases it.
T3 acquires write lock on K (now at LSN 200), writes K at LSN 300.
T3 commits (TxnCommit at LSN 350).
System crashes.

Recovery:
  Analysis: T1 → aborted, T3 → committed.
  Redo:    applies T3's committed write → slot K = (T3_data, lsn=300). ✓
  Undo:    encounters T1's LnRecord at LSN 200.
           is_committed(T1) = false → not skipped.
           compute_undo_action → UndoAction::RevertToAbortLsn { abort_lsn: pre_T1 }
           tree.insert(K, pre_T1_data, pre_T1_lsn)  ← unconditional overwrite
  Result:  slot K = (pre_T1_data, pre_T1_lsn).  T3's committed data GONE. ✗
```

The same defect appears in both `run_undo` (line 1643) and `run_undo_all`
(line 789) — multi-database recovery is equally affected.

There is no test that exercises this interleaving.  The new
`equality_aborted_txns` test in `recovery_correctness_test.rs` uses disjoint
key prefixes for committed and aborted batches so it cannot catch this case.

### Suggested fix
Add `BinStub::get_slot_lsn(full_key) -> Option<Lsn>` to `noxu-tree`.  In
`run_undo` / `run_undo_all`, before dispatching the `UndoAction`, verify:

```rust
if let Some(current) = tree.get_slot_lsn(&rec.key) {
    if current != pe.lsn {
        continue; // slot already at a newer version — skip
    }
}
```

Alternatively, expose `Tree::undo_insert(key, abort_data, abort_lsn,
log_lsn)` and `Tree::undo_delete(key, log_lsn)` that atomically check the
slot LSN under the BIN write lock before applying, mirroring JE's
`bin.recoverRecord`.

---

## F-2 — Critical: SERIALIZABLE Range Locks Never Acquired; Phantom Prevention Absent

### Files:lines (main)
`crates/noxu-dbi/src/cursor_impl.rs:997–1035` (`lock_ln` — always acquires
`LockType::Read` regardless of isolation level).
`docs/src/transactions/isolation.md:15,109,118`.
`docs/src/transactions/durability.md:42,79`.

### What JE does
`Cursor.java:5280`:
```java
return rangeLock ? LockType.RANGE_READ : LockType.READ;
```
When `isSerializableIsolation()` is true, `rangeLock=true` is passed to
`getLockType()` and reads acquire `RANGE_READ` instead of `READ`.  A
concurrent insert acquires `RANGE_INSERT`; `LockConflict.RESTART` between
`RANGE_READ` and `RANGE_INSERT` triggers a cursor restart, making the phantom
visible to the application.  End-of-range is guarded by `lockEof(RANGE_READ)`.

### What Noxu does
`cursor_impl.rs:lock_ln` acquires `LockType::Read` unconditionally:

```rust
// line 1011 — same path regardless of is_serializable_isolation()
let contended = match guard.lock(lsn, LockType::Read, true) { ... };
```

`RANGE_READ`, `RANGE_WRITE`, and `RANGE_INSERT` are defined in
`lock_type.rs` with the correct JE-compatible conflict matrix and appear in
`lock_impl.rs` and `thin_lock_impl.rs`.  However a codebase-wide search
finds **zero calls** to any range lock type from `noxu-dbi` or `noxu-db`.
The range lock infrastructure is dead code at the operational level.

### The unsubstantiated claims
The documentation explicitly asserts the guarantee:
- `isolation.md:15`: "Noxu DB prevents phantoms with **additional range locking**."
- `isolation.md:109`: "Serializable isolation prevents **phantom reads**."
- `isolation.md:118`: "Serializable isolation causes additional locking (**range locks**) which can reduce throughput."
- `durability.md:42`: "| Serializable | Highest (**range locks**) | None |"

All four claims are false.  A second range-read under `serializable_isolation
= true` will observe insertions by concurrent committed transactions.

### Suggested fix
In `cursor_impl.rs::lock_ln`, select the lock type based on isolation:
```rust
let lock_type = if guard.is_serializable_isolation() {
    LockType::RangeRead
} else {
    LockType::Read
};
```
For phantom-complete serializability also wire `RangeInsert` for puts and a
`lock_eof(RangeRead)` call at scan end of range, mirroring
`CursorImpl.lockEof` in JE.  Until this is implemented, downgrade the
documentation claim to "repeatable-read" (read locks held for txn duration,
phantoms not prevented).

---

## F-3 — High: Checkpointer Writes `first_active_lsn = Lsn::new(0,0)` in CkptEnd; Recovery Always Scans Entire Log

### File:line (main)
`crates/noxu-recovery/src/checkpointer.rs:466–477` (CkptEnd construction).

### What JE does
`Checkpointer.java:795–800` (annotated with bug reference `[#20270]`):
```java
// Retrieve AFTER logging CkptStart, not before (ordering matters).
long firstActiveLsn = envImpl.getTxnManager().getFirstActiveLsn();
if (firstActiveLsn == DbLsn.NULL_LSN) {
    firstActiveLsn = checkpointStart;
}
```
This value is stored in `CheckpointEnd` and bounds both the undo backward-scan
stop point and the LN-redo forward-scan start point during crash recovery.

### What Noxu does
```rust
// checkpointer.rs:466–477
// Set first_active_lsn to Lsn::new(0, 0) (beginning of log)
// rather than NULL_LSN. This tells recovery to scan from the start of
// the log, ensuring that committed LN entries written before the
// checkpoint start are still replayed.
// (JE would set this to the LSN of the earliest active txn at
// checkpoint time; Noxu conservatively uses Lsn::new(0,0) until the
// checkpoint wires the full db_map.)
noxu_util::Lsn::new(0, 0), // first_active_lsn
```

The project's own documentation acknowledges the P-2 gap
(`docs/src/internal/wave-gb-dbtree-recovery.md:207–220`) and records that:

> "The checkpointer currently has no connection to the transaction manager.
> Implementing this connection is a follow-on prerequisite."

The `fix/gb-dbtree-recovery` branch prototyped the fix but it was not merged
to main.

### Why it is a problem
Recovery time is **O(total log size)**, not O(checkpoint interval).  For a
database that runs continuously and accumulates many log files, every restart
— even after a clean shutdown + checkpoint — requires a full scan from file 0,
offset 0.  Recovery time grows without bound as the database ages.

There is also an additional correctness dimension documented in
`wave-gb-dbtree-recovery.md:185–200`: if a transaction begins before
`CkptStart` and crashes without a commit or abort record, and if recovery
were ever optimised to start from `CkptStart` rather than `0,0`, that
transaction's writes would not be undone, silently surfacing uncommitted data.
The current `Lsn::new(0,0)` approach avoids this correctness violation at
the cost of unbounded recovery time.  Fixing F-3 requires fixing F-4 first.

### Suggested fix
1. Fix F-4 (wire `update_first_lsn` calls from cursor layer).
2. Pass the `Arc<TxnManager>` into `Checkpointer` (or pass the result of
   `get_first_active_lsn()` as a parameter to `do_checkpoint`).
3. Compute `first_active = txn_mgr.get_first_active_lsn()` **after** writing
   the `CkptStart` record (JE `[#20270]` ordering invariant), defaulting to
   `checkpoint_start` on `NULL_LSN`.
4. Write that value into `CkptEnd`.

---

## F-4 — High: `TxnManager::update_first_lsn` Defined but Never Called from Cursor Layer

### File:line (main)
`crates/noxu-txn/src/txn_manager.rs:156` (method definition, zero call sites outside tests).
`crates/noxu-dbi/src/cursor_impl.rs:602` (`note_log_entry` updates `Txn`-internal
state only, does not propagate to `TxnManager`).

### What JE does
`Txn.java` tracks `firstLoggedLsn` per-transaction.  `TxnManager.getFirstActiveLsn()`
iterates `allTxns.keySet()` and calls `txn.getFirstActiveLsn()` on each live
transaction to return the minimum.  The checkpointer then consumes this value
(see F-3).

### What Noxu does
When a cursor writes an LN, `cursor_impl.rs:602` calls
`guard.note_log_entry(new_lsn_u64)`, which correctly updates `Txn::first_lsn`
and `Txn::last_lsn`.  However there is **no call to
`TxnManager::update_first_lsn`** anywhere in `noxu-dbi` or `noxu-db`
(confirmed by codebase-wide search).  The `all_txns` map in `TxnManager`
records `NULL_LSN` for every transaction's first-lsn, permanently.
Consequently `get_first_active_lsn()` always returns `NULL_LSN`.

The `TxnManager::update_first_lsn` docstring claims:

> "Called by `Txn` when it writes its first log entry. This allows
> `get_first_active_lsn()` to return the correct lower bound for
> checkpointing."

This documented contract is entirely unimplemented — the wiring is absent.

### Suggested fix
In `cursor_impl.rs`, in the write-path where `note_log_entry` is called,
also call `txn_manager.update_first_lsn(txn_id, new_lsn_u64)` on the first
write (i.e. when `guard.first_lsn() == NULL_LSN.as_u64()` before the call).
This requires threading the `TxnManager` reference into `CursorImpl`.

---

## F-5 — High: `TxnManager::commit_txn`/`abort_txn` Not Called for Explicit Transactions; Memory Leak and Incorrect Stats

### File:line (main)
`crates/noxu-db/src/transaction.rs` (commit/abort paths — no call to
`TxnManager::commit_txn` or `abort_txn`).
`crates/noxu-db/src/database.rs:323,328,341` (only auto-commit callers).

### Context on main vs stale branch
Main introduced `noxu-db::ActiveTxns` (environment.rs:125–152) and
`Transaction::with_active_txns` / `registry.mark_complete` (transaction.rs:477–483,
661–664) so that `env.close()` correctly detects open explicit transactions.
**This fixes the env.close() correctness bug** that existed on the stale branch.

The separate `TxnManager::all_txns` map (in `noxu-txn`) is **still not
cleaned up** for explicit transactions.  `TxnManager::commit_txn` and
`abort_txn` are called only from `database.rs` for synthetic auto-commit
transactions.

### What JE does
`Txn.close()` (called from `abortInternal` and commit) calls
`txnManager.unregisterTxn(this)` which removes the entry from `allTxns`.

### Why it is a problem
1. **Memory leak**: `TxnManager::all_txns` grows monotonically at one entry
   per explicit transaction, never shrinking.  In a long-running process this
   is an unbounded `HashMap` growth.
2. **Incorrect stats**: `EnvironmentImpl::n_active_txns()` (line 1719) calls
   `txn_manager.n_active_txns()`, which counts `all_txns.len()` — it grows
   without bound and never reflects the true count of live transactions.
   External monitoring tools reading this value will see garbage.
3. **Perpetually-incorrect serializable counter**:
   `TxnManager::register_serializable` and `unregister_serializable` are
   never called from the `noxu-db` layer (confirmed by codebase search).
   The `are_other_serializable_transactions_active()` check used by the
   evictor therefore perpetually returns `false` — even when serializable
   transactions are running — starving the evictor of a correct signal.
4. **Deadlock diagnostics degraded**: `locker_labels` in `LockManager` are
   registered via `register_locker_label` in `begin_txn` but
   `unregister_locker_label` (called from `commit_txn`/`abort_txn`) is never
   reached for explicit transactions, causing the label map to grow without
   bound.

### Suggested fix
In `Transaction::commit_with_durability` and `Transaction::abort`, after the
inner `Txn` finalises, call:
```rust
env_impl.get_txn_manager().commit_txn(self.id);  // or abort_txn
env_impl.get_txn_manager().unregister_locker_label(self.id);
```
Also call `register_serializable()` / `unregister_serializable()` in
`begin_transaction` / commit/abort for serializable transactions.

---

## F-6 — Medium: `Txn::retains_locks_on_commit()` Returns `true` for Serializable but Commit Drains All Read Locks Unconditionally

### File:line (main)
`crates/noxu-txn/src/txn.rs:1405` (`retains_locks_on_commit` returns
`self.serializable_isolation`).
`crates/noxu-txn/src/txn.rs:385–397` (unconditional `read_locks.drain()` in
`commit_with_durability`).

### The claim vs. the code
```rust
// txn.rs:1405
fn retains_locks_on_commit(&self) -> bool {
    self.serializable_isolation  // returns true for SERIALIZABLE
}
```
`Locker` trait doc (`locker.rs:62`):
> "Returns true if locks should be **retained on commit** (serializable isolation)."

But `commit_with_durability` at line 385:
```rust
for lsn in self.read_locks.drain().collect::<Vec<_>>() {
    // unconditional — no check of retains_locks_on_commit()
    self.lock_manager.release(lsn, self.id)?;
}
```
A codebase-wide search shows `retains_locks_on_commit()` is **never called
from any commit, cursor, or database code path** — only in `locker.rs` tests.
The return value is dead.

### Why it is a problem
This inconsistency has two operational consequences:

1. **Misleading API surface**: any future caller (replication layer, secondary
   index manager) inspecting `retains_locks_on_commit()` to decide whether
   this transaction's read locks survive commit will receive `true` for
   serializable transactions, and act on a false premise.  The locks will
   already be gone.

2. **Incorrect design intent**: if the intent is that serializable
   transactions hold read locks past commit (the stated contract), then F-2
   (using `RangeRead` instead of `Read`) is also wrong, and vice versa — the
   two bugs are design-inconsistent.  JE does *not* retain plain read locks
   past commit for serializable; it uses `RANGE_READ` locks that prevent
   phantom inserts during the transaction lifetime, then releases all locks on
   commit.

### Suggested fix
Either:
1. Change `retains_locks_on_commit()` to return `false` (matching JE and the
   actual commit behaviour), and fix F-2 to deliver real serializable isolation
   via range locks; or
2. Make `commit_with_durability` honour the return value — but only after F-2
   is implemented, since holding plain `Read` locks past commit is both
   unnecessary and non-JE-compatible.

---

## F-7 — Medium: `lock_with_sharing_and_timeout` Skips Per-Wakeup Deadlock Re-Check

### File:line (main)
`crates/noxu-txn/src/lock_manager.rs:877–891` (`lock_with_sharing_and_timeout`
wait loop).
`crates/noxu-txn/src/lock_manager.rs:430–470` (`lock_with_timeout` wait loop, for
comparison).

### The divergence
`lock_with_timeout` (line 431–465): on every slice expiry or spurious wakeup,
drops `granted_guard`, queries current owner IDs from the shard, and runs the
cycle-detection algorithm:
```rust
// Every iteration:
drop(granted_guard);
{ /* check_deadlock_for_waiter with fresh owner list */ }
granted_guard = mutex.lock();
```

`lock_with_sharing_and_timeout` (line 877–891): deadlock detection only fires
when `timed_out.timed_out()` is true — spurious wakeups fall through without
a cycle check:
```rust
let timed_out = condvar.wait_for(...);
if timed_out.timed_out()
    && let Some(dl_err) = self.check_deadlock_for_waiter(...) {
    return Err(dl_err);
}
// no check on spurious/notified wakeup
```

### Why it is a problem
`lock_with_sharing_and_timeout` is used by `ThreadLocker` and `HandleLocker`
when those lockers are in a sharing group — a common path for multi-cursor
operations.  A deadlock that forms after the waiter enters the wait path will
not be detected on the next wakeup; detection is deferred until the next 50 ms
slice expires.  In the worst case (both sides wake spuriously on every slice)
the deadlock is not caught for up to `lock_timeout_ms` milliseconds rather
than one 50 ms slice.  The asymmetry also makes the codebase harder to reason
about: two wait loops with structurally identical intent but subtly different
detection latency.

### Suggested fix
Mirror `lock_with_timeout`'s pattern in the sharing wait loop: drop
`granted_guard`, re-query current owner IDs from the shard, call
`check_deadlock_for_waiter`, then re-acquire `granted_guard` on every
iteration regardless of timeout status.

---

## F-8 — Medium: Documentation Claims Range Locks for SERIALIZABLE; Code Delivers Plain Read Locks

### Files:lines (main)
`docs/src/transactions/isolation.md:15` ("Noxu DB prevents phantoms with additional range locking").
`docs/src/transactions/isolation.md:109` ("Serializable isolation prevents phantom reads").
`docs/src/transactions/isolation.md:118` ("Serializable isolation causes additional locking (range locks)…").
`docs/src/transactions/durability.md:42` ("Serializable | Highest (range locks) | None").
`docs/src/transactions/durability.md:79` ("Prevent phantom reads | TransactionConfig::with_serializable_isolation(true)").

All five claims are false on main (see F-2 for the code evidence).  A user
who configures `serializable_isolation = true` trusting the documented
guarantee will observe phantom reads.

This is treated as a separate finding from F-2 because it is an unsubstantiated
claim in user-facing documentation — a distinct production-readiness failure from
the missing code.

### Suggested fix
Until F-2 is implemented, update the documentation:
- Replace "range locking" with "read-lock retention" (what is actually
  delivered: read locks held for the full transaction duration, not released
  early as in READ_COMMITTED).
- State explicitly that phantom reads are **not currently prevented**.
- Add a `<!-- TODO: update when range locks are wired -->` comment to each
  occurrence so a future PR implementing F-2 can find and update them.

---

## F-9 — Medium: Stale TODO in Test Comment Claims Open Bug; Defensive Fix Is Already In Place

### File:line (main)
`crates/noxu-recovery/tests/prop_tests.rs:376–383` (TODO comment claiming
`record_active_txn` hardening "hasn't been decided").
`crates/noxu-recovery/src/analysis_result.rs:292–306` (defensive guard that
resolves the bug).

### What the comment says
```rust
// prop_tests.rs:376
/// TODO: decide whether `record_active_txn` should be
/// hardened with a defensive `if is_committed || is_aborted { return; }`,
/// or whether the precondition should be promoted to a `debug_assert!`
/// and callers audited.
```

### What the code already does
`analysis_result.rs:292–304`:
```rust
pub fn record_active_txn(&mut self, txn_id: u64) {
    if self.committed_txns.contains_key(&txn_id)
        || self.aborted_txns.contains(&txn_id)
    {
        return;  // ← the "hardened" guard IS present
    }
    ...
}
```
The test at `prop_tests.rs:380` asserts the bug-free behaviour (`has_active_txns()
== false` after `record_commit` + `record_active_txn`), and the test **passes**
with the current implementation.

### Why it is a problem
A reader of the test comment file will believe the phantom-active-txn
behaviour is an open bug and may waste time investigating, or may
incorrectly remove the defensive guard as "unneeded overhead pending a
decision."  The comment also contains the line "the impl says it's true" (line
374), which is no longer accurate.

### Suggested fix
Delete the TODO block (lines 376–383).  Update the test docstring to describe
what is being **verified** (the fixed behaviour), not what the bug was.
Optionally upgrade the guard to `debug_assert!(!is_committed && !is_aborted)`
with an invariant message.

---

## F-10 — Low: `Txn::lock_after_lsn_change` Is Dead Code with Both Error Paths Silently Swallowed

### File:line (main)
`crates/noxu-txn/src/txn.rs:1414–1445` (`lock_after_lsn_change` in the `Locker`
impl for `Txn`).

### The defect
```rust
fn lock_after_lsn_change(&mut self, old_lsn: u64, new_lsn: u64) -> Result<(), TxnError> {
    if let Some(wli) = self.write_locks.remove(&old_lsn) {
        let _ = self.lock_manager.release(old_lsn, self.id);  // error ignored
        let _ = self.lock_manager.lock(new_lsn, ...);          // error ignored
        self.write_locks.insert(new_lsn, wli);
    }
    Ok(())
}
```

Both errors are dropped with `let _ = ...`.  If the new LSN lock acquisition
fails, `write_locks` will contain a `WriteLockInfo` at `new_lsn` while the
lock manager holds no lock there.  A subsequent `abort()` will attempt
`release(new_lsn)` against a lock the manager does not own, silently failing,
and the before-image in `WriteLockInfo` will be orphaned.

Compare with the correct pattern in `move_write_lock_to_new_lsn` (same file),
which propagates errors and guarantees `WriteLockInfo` is never silently
desynchronised from the lock manager.

### Why severity is Low
A codebase-wide search confirms **`lock_after_lsn_change` has no callers**
outside the trait definition and `locker.rs` unit tests — it is dead code.
The defect cannot be triggered in the current build.

### Suggested fix
Replace the body with:
```rust
// Callers should use move_write_lock_to_new_lsn, which propagates errors.
unimplemented!("use Txn::move_write_lock_to_new_lsn instead")
```
Or remove the method from the `Locker` trait if no implementor needs it.

---

## Testing Gaps (Appendix)

The following isolation/recovery scenarios are documented in the API or implied
by the code architecture but have **no test** anywhere in the main repository:

1. **F-1 exact scenario** (Critical gap): `T1 writes K → T1 aborts → T3
   commits K → crash → reopen`.  Assert `K == T3_data` after recovery.
   The new `equality_aborted_txns` test uses disjoint key namespaces for the
   aborted and committed batches; it **cannot** catch this case.

2. **Serializable phantom prevention** (F-2 gap): start a serializable cursor
   scan over range `[a, z]`; concurrently insert `m`; verify the first cursor
   observes a restart signal or that `m` is not visible on a second pass
   within the same transaction.  No test exists.

3. **Deadlock detection in `lock_with_sharing_and_timeout`** (F-7 gap): no
   test exercises the case where a deadlock forms between two sharing-group
   lockers after the waiter enters the wait path.  `test_deadlock_detected`
   only exercises `lock_with_timeout`.

4. **`firstActiveLsn` ordering invariant** (`[#20270]` class): no test verifies
   that a transaction begun before `CkptStart` and committed after it is
   correctly recovered after a crash between the two checkpoint markers.  The
   new `open_txn_spanning_checkpoint_recovers_correctly` test covers the
   uncommitted-txn case; the committed-after-start case is untested.

---

*Report path*: `/tmp/review-txn-isolation.md`
*Based on*: `/tmp/noxu-review` (origin/main, v3.2.0, commit `34171f6`)
*Generated*: 2026-06-03
