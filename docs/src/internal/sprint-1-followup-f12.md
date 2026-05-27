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

*Both residuals listed in this section were closed in Wave 1A of the
v1.5.1 polish work; see [Resolution (Wave 1A)](#resolution-wave-1a)
below.*

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

## Resolution (Wave 1A)

Both residuals were closed on branch `fix/wave1a-f12-residuals` (merged
as part of v1.5.1).  The fix follows exactly the suggested approach
below:

* `TxnManager::begin_auto_txn(env_log_manager)` allocates a transient
  synthetic `Txn` from the same `next_txn_id` counter as explicit
  transactions.  The auto-txn carries a new `IS_AUTO_TXN` flag so
  `Txn::commit_with_durability` and `Txn::abort` skip the
  `TxnCommit` / `TxnAbort` WAL entries (the underlying LN was logged
  as auto-commit `InsertLN` / `DeleteLN` with `txn_id = 0`, so the
  on-disk WAL format is unchanged); the auto-commit fsync formerly
  in `Database::auto_commit_sync` is folded into
  `Txn::commit_with_durability` so explicit and synthetic-auto txns
  share one durability path.
* `Database::with_auto_txn` wraps every `txn = None` write
  (`put`, `put_no_overwrite`, `delete`) in such a synthetic auto-txn
  attached to the cursor via `CursorImpl::attach_txn`.  On `Ok` the
  auto-txn commits; on `Err` it calls `abort_collect_undo` and
  `apply_auto_txn_undo` rolls the in-memory tree write back through
  `WriteLockInfo` before locks are released — so a forced mid-write
  failure in auto-commit cannot leave a partial in-memory write.
* `CursorImpl::lock_write_before_log` now takes the key as a second
  argument and, when `old_lsn == NULL_LSN` (a brand-new insert),
  acquires a write lock on a synthetic key-coordination LSN derived
  from `(database_id, key)` via the new
  `noxu_util::Lsn::synthetic_key_lock_id(db_id, key)`.  The encoding
  uses the reserved `MAX_FILE_NUM` (`0xFFFFFFFF`) as the high 32
  bits, so a synthetic key lock id can never collide with a real
  WAL LSN.  Two concurrent auto-commit inserts of the same
  brand-new key now serialise through the lock manager and the
  loser observes `KeyExist` after re-checking `key_exists_in_view`
  under the held lock.  `CursorImpl::put`'s
  `PutMode::NoOverwrite` / `PutMode::NoDupData` paths run that
  re-check unconditionally so the loser also bails when it lost on
  a real-LSN write lock (i.e. the winner had already populated the
  slot before the loser captured `old_lsn`).
* `LockManager` grows a typed-label registry
  (`register_locker_label`, `unregister_locker_label`,
  `format_locker`, `format_lockers`) populated by
  `TxnManager::begin_txn` (label `"txn"`) and
  `TxnManager::begin_auto_txn` (label `"auto-txn"`).  The deadlock
  cycle list and the `LockTimeout::owner` / `LockTimeout::requester`
  fields now render typed identifiers like `"auto-txn:42"` and
  `"txn:17"` instead of opaque integers — closing the
  locker-id-collision-space residual.

### Verification

New tests in
[`crates/noxu-db/tests/wave1a_f12_residuals_test.rs`](https://codeberg.org/gregburd/noxu/src/branch/main/crates/noxu-db/tests/wave1a_f12_residuals_test.rs):

1. `null_lsn_insert_race_two_auto_commit_inserts_serialise_through_lock_manager`
   — 64 rounds of two-thread brand-new-key races assert that exactly
   one thread sees `Success`, the other sees `KeyExists`, and the
   stored value is the winner's.
2. `null_lsn_insert_race_recovery_has_no_phantom_keys` — the same
   race followed by `Environment::close()` + reopen verifies the
   winners persist across recovery and no phantom keys appear.
3. `auto_commit_rollback_on_forced_failure_undoes_in_memory_write`
   — uses the `set_cursor_fail_after` test hook to inject a
   mid-write failure during auto-commit and asserts the in-memory
   tree is rolled back via the synthetic auto-txn's abort-undo.
4. `auto_commit_single_thread_performance_regression_check` — 1024
   sequential auto-commit puts and round-trips, a liveness check
   that the synthetic-auto-txn rewrite did not catastrophically
   regress single-thread auto-commit.
5. `lock_timeout_message_uses_typed_locker_ids` — deterministically
   provokes a `LockTimeout` between an explicit txn and an
   auto-commit op and asserts the error message body contains both
   `"auto-txn:"` and `"txn:"`.

All tests pass; the existing F12 wiring tests in
`crates/noxu-db/tests/txn_wiring_test.rs` continue to pass
unchanged.

## Suggested follow-up for v1.5.x

*Closed in Wave 1A.  Retained for historical context.*

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
