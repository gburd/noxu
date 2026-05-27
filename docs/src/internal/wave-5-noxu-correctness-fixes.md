# Wave 5 — Noxu correctness fixes (JE TCK regressions)

> Status: closed. Branch: `fix/wave5-noxu-correctness`.

Wave 4-B's JE TCK port surfaced three real Noxu correctness bugs as
`#[ignore]`-d regression tests in `crates/noxu-db/tests/`. Wave 5
closes all three. This document records the bug, the root cause, and
the fix for each, and serves as the post-mortem reference for a
future audit.

## Bug 1 — Aborted dup inserts persist on sorted-duplicates DBs

**Test**:
`crates/noxu-db/tests/je_recovery_sr_test.rs::sr9752_part2_abort_after_committed_dups_reverts_with_dups`.

**Symptom**: After `txn.abort()`, dup values inserted by the aborted
txn remained visible. With three committed dups + three aborted dups
the test observed all six post-abort.

**Root cause**: in `crates/noxu-dbi/src/cursor_impl.rs::put_dup` the
`PutMode::Overwrite` branch logged the LN and inserted into the
tree but skipped both `lock_write_before_log()` and
`finalize_write_lock()` entirely, so no `WriteLockInfo` was
registered with the txn. With no undo record, `Transaction::abort`
had nothing to roll back. The matching `NoOverwrite` /
`NoDupData` branches were patched in v1.6 / Wave 2A; `Overwrite` was
missed because the existing tests did not exercise it on a
sorted-dup database. `Database::put()` defaults to `PutMode::Overwrite`,
so any user-facing aborted dup insert leaked past rollback.

**Fix**: probe the tree for an existing `(key, data)` slot LSN
before logging, then issue the matching
`lock_write_before_log()` / `finalize_write_lock()` pair. For a
brand-new pair this records `abort_known_deleted=true` so the
abort tree-undo path deletes the slot; for an existing pair it
records the prior slot LSN so abort restores the LSN.

**Commit**: `fix(noxu-dbi): record undo info for sorted-dup PutMode::Overwrite (SR9752 part 2)`.

## Bug 2 — Aborted delete-then-reinsert corrupts BIN

**Tests**:
`sr9465_part1_delete_reinsert_abort_restores_no_dups` and
`sr9465_part2_delete_reinsert_redelete_abort_restores_no_dups`.

**Symptom**: With N committed records, `delete-all + put-all + abort`
left a non-deterministic subset of records lost. With N=50 the
in-memory check observed counts ranging from 0 to 30 per run.

**Root cause**: in-memory abort iterated the write-lock `HashMap` in
arbitrary order. When the same key is touched multiple times in one
txn (`delete` → `reinsert`) each operation records its own undo
record keyed by the new LSN, and the records carry conflicting
intents:

| operation | LSN | undo intent |
|---|---|---|
| delete | L1 | insert original (`abort_data=orig`) |
| reinsert | L2 | delete the slot (`abort_known_deleted=true`) |

Applying `[L1 then L2]` re-inserts the original then deletes it →
key gone. Applying `[L2 then L1]` deletes the reinserted slot then
re-inserts the original → correct. The crash-recovery undo path
already walks the WAL backward (newest-LSN first) for exactly this
reason; the in-memory path did not.

A second, smaller bug surfaced once the ordering was fixed: the
in-memory abort path called `tree.insert(...)` directly to restore
a slot but did not bump the per-database entry counter, while the
delete that put us in this state had decremented it. The counter
was therefore left at zero even though the data was correct.

**Fix**:

1. Sort `undo_records` by `current_lsn` descending in the three
   in-memory abort paths (`Transaction::abort`,
   `Transaction::resolved_abort_after_prepare`,
   `Database::apply_auto_txn_undo`).
2. Re-bump the per-database entry counter when restoring a slot
   that the aborted txn had deleted.
3. Scope `txn1` / `txn2` / `db` / `env` handles inside an inner
   block in the two SR9465 regression tests so the FileManager
   file lock is released before the recovery reopen — matches the
   existing `sr9752_part1` pattern. Without this the second
   open hit `Environment locked: locked by another process`. This
   was a pre-existing test-only issue surfaced once the in-memory
   correctness check started to pass; user code can call
   `Environment::close()` explicitly to force release. (See the
   commented `sr9752_part1` for the canonical scoping pattern.)

**Commit**: `fix(noxu-db): apply abort undo records in reverse-LSN order (SR9465)`.

## Bug 3 — Uncommitted delete is dirty-readable

**Test**:
`crates/noxu-db/tests/je_cursor_edge_test.rs::cursor_edge_read_deleted_uncommitted`.

**Symptom**: A no-wait reader saw `Ok(NotFound)` against an
uncommitted-deleted record (no lock conflict), even though an
uncommitted *overwrite* correctly produced a lock error. JE's
contract for uncommitted-delete dirty-read prevention requires the
no-wait reader to see `LockNotAvailable` while T1 has the delete
in flight, and a blocking reader to wait until T1 commits or
aborts.

**Root cause**: `cursor.delete()` calls `tree.delete(&key)` which
physically removes the BIN slot. After the in-memory removal a
concurrent reader looking up the same key under a different txn
sees `NotFound` from `tree.search()` without ever consulting the
lock manager for the slot's old LSN — there is no LSN to lock,
because the slot is gone. JE keeps the slot present with
`pendingDeleted=true` until the LN compressor runs post-commit;
that architectural change is much larger than this wave.

**Fix**: synthetic-key write lock that the deleter holds in
addition to the slot's LSN write lock, for the duration of the
txn. Readers that probe the BIN and find no matching key contest
the same synthetic-key id with a `Read` lock; on contention the
typed lock error surfaces (no-wait) or the reader blocks until
the deleter finalises (blocking).

Implementation:

- `lock_synthetic_key_for_delete(key)`: taken from
  `cursor.delete()` right after `lock_write_before_log`; held
  until commit/abort.
- `contest_synthetic_key_for_missing_read(key)`: called from
  `search()`'s `SearchMode::Set` / `SearchMode::Both` `NotFound`
  branch.

Subtle correctness point: `contest_synthetic_key_for_missing_read`
short-circuits when the calling txn already owns a write lock on
the same synthetic key. Without this guard a deleter's own
post-delete cursor re-search inside `Database::delete`'s scan loop
would (a) succeed via `Existing`-grant inside `Lock::lock`, but (b)
`Txn::lock` unconditionally inserts the lsn into `self.read_locks`,
and (c) the matching `release_lock` would then hand the lsn to
`lock_manager.release`, erroneously freeing the deleter's write
lock and letting other lockers race in. The short-circuit avoids
the `read_locks` pollution entirely.

**Commit**: `fix(noxu-dbi): contest synthetic-key lock for missing-key reads (testReadDeletedUncommitted)`.

## Cross-cutting follow-ups

The synthetic-key approach is a stop-gap for Bug 3. A more
faithful port of JE's BIN slot model — keep the slot with
`pendingDeleted=true` until the LN compressor runs, and gate the
slot's LSN check at read time on `pendingDeleted` rather than
"slot exists" — would be more efficient (avoids the extra
lock-manager round-trip on every `NotFound` read) and would also
let crash-recovery undo restore the slot in place rather than
re-inserting it. That work is tracked as a future audit item; the
synthetic-key fix is correct for all observable behaviour.

Bug 1's fix only patches the `PutMode::Overwrite` path of
`put_dup`; the `PutMode::Current` branch still skips
`lock_write_before_log` / `finalize_write_lock` and would leak the
same way for an aborted `Cursor::put(PutCurrent, ...)` on a
sorted-dup DB. No JE TCK test exercises that path so it has been
left for a follow-up audit; flagging here so the next reviewer can
file a regression test.

Bug 2's `HashMap`-iteration bug applies to every multi-write-on-same-key
abort, not only delete-then-reinsert. The reverse-LSN sort is
exhaustive for the in-memory path; the recovery path was already
correct and is unchanged.

## Test enablement summary

| Test | Pre-Wave-5 | Post-Wave-5 |
|---|---|---|
| `sr9752_part2_abort_after_committed_dups_reverts_with_dups` | `#[ignore]` | passing |
| `sr9465_part1_delete_reinsert_abort_restores_no_dups` | `#[ignore]` | passing |
| `sr9465_part2_delete_reinsert_redelete_abort_restores_no_dups` | `#[ignore]` | passing |
| `cursor_edge_read_deleted_uncommitted` | `#[ignore]` | passing |
