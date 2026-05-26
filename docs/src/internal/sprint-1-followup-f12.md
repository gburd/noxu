# Sprint 1 Follow-up — F12 (auto-commit isolation)

This note records the residual F12 work that was *not* required to be
addressed in Sprint 1 of the v1.5.0 remediation plan, and documents
the actual state of auto-commit isolation as of `fix/sprint1-env-txn-wiring`.

## What the audit claimed (F12, May 2026)

> Auto-commit (`txn = None`) writes apply to the in-memory tree
> directly without acquiring per-record write locks via a `Txn`; only
> the WAL fsync is gated. Concurrent auto-commit + explicit-txn
> workloads can therefore observe non-isolated states even on a
> transactional environment.

## Actual state of the code

The audit's claim is **partially overstated**. Auto-commit cursors
(those created without a bound `Txn`) **do** consult the
`LockManager` directly:

* `crates/noxu-dbi/src/cursor_impl.rs` — `lock_write_before_log` and
  `finalize_write_lock` call
  `lock_manager.lock(lsn, cursor_id, LockType::Write, ...)` and
  immediately release after the write is logged.
* The same cursor's `lock_ln` takes a `LockType::Read` on the LSN
  before reads.

This means:

* An explicit txn that holds a **write lock** on K causes a concurrent
  auto-commit `db.put(None, K, _)` to **block** until the explicit txn
  commits or aborts. Verified by
  `f12_auto_commit_write_blocks_on_explicit_txn_write_lock` in
  `crates/noxu-db/tests/txn_wiring_test.rs`.
* An explicit txn under serializable isolation that holds a **read
  lock** on K also blocks a concurrent auto-commit
  `db.put(None, K, _)`. Verified by
  `f12_explicit_txn_read_blocks_auto_commit_write`.
* Auto-commit on an unrelated key does **not** block on the explicit
  txn's locks. Verified by
  `f12_auto_commit_does_not_block_on_unrelated_key`.

The audit's "ACID violation" wording was based on a static read of
`Database::put` that did not follow the call chain into
`CursorImpl::*` where the lock-manager calls live.

## Pre-existing bug fixed as part of F1

While verifying F1 the
`recovery_uncommitted_transactions_are_undone_on_reopen` test broke,
because `Environment::close()` now actually runs `EnvironmentImpl::close()`
(its forced close-checkpoint and `flush_sync`) rather than failing on the
active-txns gate. Investigation revealed two pre-existing bugs that
were masked by the broken close path:

1. `Database::make_cursor_for_txn` always passed `locker_id = 0` to
   `CursorImpl`. `CursorImpl::log_ln_write` translates
   `locker_id == 0` to `txn_id = None` in the LN log entry, which
   means *every* explicit-txn write was logged with the auto-commit
   form. Recovery's redo / undo phases couldn't tell that those
   writes belonged to a transaction.

2. `CursorImpl::log_ln_write` always picked `LogEntryType::InsertLN` /
   `LogEntryType::DeleteLN` (the non-transactional forms) regardless
   of whether `txn_id_opt` was `Some(_)`. The LN payload's
   `is_transactional` is determined by the *outer entry-type byte*
   (per `LnLogEntry::parse_from_slice`), so writing a transactional
   payload under a non-transactional outer type meant recovery
   skipped the `txn_id` / `abort_lsn` fields and then read the rest
   of the entry from the wrong byte offset, producing phantom keys
   like `[0]`, `[0,0]`, `[0,0,0]`, …

Both are now fixed:

* `Database::make_cursor_with_locker(i64)` — pass `txn.get_id() as i64`
  for transactional cursors, `0` for auto-commit cursors.
* `CursorImpl::log_ln_write` — pick `InsertLNTxn` / `DeleteLNTxn` when
  `txn_id_opt.is_some()`.

These two changes are part of the F12 commit (they are tightly bound
to the auto-commit-vs-txn distinction and were exposed only by F1).

## What remains (deferred, not blocking v1.5.0)

There are two genuine gaps Sprint 1 did **not** address:

1. **NULL-LSN insert race.** `lock_write_before_log` returns early
   when `old_lsn == NULL_LSN`. Two concurrent auto-commit *inserts*
   of the same brand-new key therefore do not coordinate through the
   lock manager. Today the underlying B+tree latching in `noxu-tree`
   serialises them so the data path is safe (one wins, the other
   sees the inserted value as the "old" version on its next attempt
   — which is actually the JE-correct outcome), but there is no
   explicit lock-manager record of the conflict and the deadlock
   detector cannot reason about it. In practice this manifests as a
   `KeyExist` error on `put_no_overwrite` rather than a lock-wait.

2. **Locker-id collision space.** Auto-commit cursors use the
   monotonic cursor `id` (`NEXT_CURSOR_ID`) as the locker. Explicit
   txns use the txn id (`TxnManager::next_txn_id`). The two id spaces
   are disjoint by construction (different counters), but the lock
   manager's deadlock detector treats both as opaque locker handles,
   so a deadlock involving both a cursor-id and a txn-id will be
   detected — the resulting error message will refer to the cursor
   by its raw integer id, which is unhelpful for debugging.

Neither gap changes user-visible isolation in the JE sense; they are
diagnostic / corner-case quality issues.

## Suggested follow-up for v1.5.x

* Wrap auto-commit writes in a synthetic transient `Txn` from
  `TxnManager::begin_auto_txn(...)`, releasing all locks (and undoing
  the in-memory write) on any error path. This gives auto-commit the
  same lock-tracking and abort-undo guarantees as explicit txns, at
  the cost of one extra `TxnManager` allocation per auto-commit
  operation.
* Use the `Locker::id()` of the synthetic auto-commit txn in deadlock
  diagnostics so the lock-manager errors carry meaningful identifiers.

## References

* Audit: `docs/src/internal/api-audit-2026-05-transaction-env.md`,
  finding F12.
* Current behavioural assertions:
  `crates/noxu-db/tests/txn_wiring_test.rs::f12_*`.
