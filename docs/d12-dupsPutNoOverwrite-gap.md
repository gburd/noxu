# Part 5 — D12: dupsPutNoOverwrite concurrent lock (DOCUMENTED GAP)

## Status: DOCUMENTED — not shipped

## JE behavior

`CursorImpl.dupsPutNoOverwrite()` acquires a "BuddyLocker" — a subsidiary
lock on the next key in the BIN — before performing the existence check and
insert.  This prevents two concurrent `NoOverwrite`/`NoDupData` operations
for the same `(sec_key, pri_key)` from both observing "not exists" and both
succeeding.

## Noxu current state

Noxu's `put_dup(NoDupData)` path (called by `insert_sec_key`) does not wire
this next-key lock.  Two concurrent auto-commit inserts for the same
`(sec_key, pri_key)` pair could theoretically both succeed.

## Why not shipped

1. The existing `lock_range_insert` + `lock_write_before_log` (synthetic-key
   lock on NULL_LSN inserts) already serializes concurrent inserts of the
   SAME two-part key through the lock manager.  The B-tree write latch
   serializes the actual slot modification.

2. The BuddyLocker in JE coordinates two threads BEFORE the B-tree latch.
   In Noxu's lock-based model, the synthetic-key lock on the two-part key
   (`lock_write_before_log` with `old_lsn = NULL_LSN`) provides the same
   pre-latch coordination: the second thread blocks on the write lock held
   by the first thread until the first commits or aborts.

3. Wiring a true next-key BuddyLocker requires:
   a. Identifying the "next key" in the BIN (requires a tree search or
      BIN traversal under latch).
   b. Acquiring a per-key lock via the lock manager with a synthetic LSN.
   c. Releasing it after the insert, coordinated with the existing
      lock-release lifecycle.
   This is non-trivial and risks subtle interactions with the deadlock
   detector.

4. The audit specifies: "If wiring a next-key lock here is clean, do it;
   if it risks the lock-sharing registry or is too invasive, document as
   a known gap and skip.  Don't ship a half-correct lock."

## Upgrade path

When Noxu adds proper BIN-level cursor-adjustment hooks (adjustCursorsForInsert
eager path), revisit whether the synthetic-key lock on the two-part key is
sufficient for the BuddyLocker race, or whether a true per-key BuddyLocker
is needed.

## Files involved

- `crates/noxu-dbi/src/cursor_impl.rs` — `put_dup(NoDupData)` path and
  `lock_range_insert` (the existing next-key locking for SERIALIZABLE
  inserts is the closest approximation).

## Reference

JE: `CursorImpl.java dupsPutNoOverwrite()` / `lockNextKeyForInsert()` /
`BuddyLocker`.
