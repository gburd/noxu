# Sprint 3 — v1.5 XA restricted to in-process only

This note records the scope decision made for Sprint 3 of the v1.5.0
remediation plan: XA distributed transactions are restricted to a
single process lifetime in v1.5, and crash-durable XA is deferred to
v2.0.

## What the audit found

The May 2026 persist/XA API audit
(`api-audit-2026-05-persist-xa.md`) flagged three issues that, taken
together, mean the v1.4 XA stack was claiming a guarantee it could
not deliver:

* **CRITICAL #1 — `xa_prepare` is not crash-durable end-to-end.**
  `xa_prepare` writes the XID to a fsync'd `PreparedLog` database
  (`_xa_prepared`), but it never tells the underlying
  `noxu-db::Transaction` it is "prepared". There is no `TxnPrepare`
  WAL record, and `noxu-recovery` does not reconstruct the prepared
  in-memory `Transaction` (write locks, undo chain, dirty pages) on
  a fresh process. After a crash, `xa_recover` returned the persisted
  XIDs but `xa_commit(xid)` returned `XaError::NotFound` because the
  in-memory `branches` map was empty.

* **CRITICAL #2 — `xa_recover` was misleading.** Documentation and
  rustdoc described `xa_recover` as "the way to discover in-doubt
  branches after a crash and resolve them." The first half was
  accurate; the second half was effectively false because of CRITICAL
  #1.

* **Finding 3 — `mark_write` footgun.** The `XaEnvironment::mark_write`
  call is what flips a prepared branch out of the read-only
  optimisation path. An application that performed writes through the
  inner `Transaction` (via `get_transaction(&xid)`) but forgot to call
  `mark_write` had its writes silently aborted at `xa_prepare` time,
  with no warning.

## Why we are not fixing CRITICAL #1 in v1.5

Implementing crash-durable XA correctly is a layered piece of work
that touches the WAL format, the recovery state machine, and the
public API:

1. **New log record `TxnPrepare(txn_id, xid)`.** Has to land in
   `noxu-log`'s entry-type table, the codec, the WAL replayer, and
   the checkpoint barrier.
2. **`noxu-txn::TxnManager` has to learn a `Prepared` state.** Today
   txns are `Open | Committed | Aborted`; we need a four-state
   machine where `Prepared` means "all locks held, undo chain
   complete, but not yet committed". Affects deadlock detection,
   close-on-error handling, and the `ActiveTxns` registry.
3. **`noxu-recovery` has to rebuild prepared txns.** During redo,
   any txn whose last logged record is `TxnPrepare` must be left in
   `Prepared` state with locks reacquired, instead of being undone
   like an ordinary in-flight transaction. That requires reading
   the lock-manager state out of the WAL — work we have not yet
   done.
4. **`XaEnvironment` has to repopulate `branches` on open.** Once
   recovery yields a list of `(xid, txn_id)` pairs, the XA layer
   needs to reconstruct `Branch` entries with the recovered
   `Transaction` so subsequent `xa_commit` / `xa_rollback` calls
   can finish the 2PC.

Each step is straightforward in isolation but the surface area is
non-trivial and intersects subsystems that Sprint 1 and Sprint 5 are
already touching. Doing it conservatively for v1.5 risks regressing
the recovery work that Sprint 1 and Sprint 5 have stabilised; doing
it aggressively risks shipping a half-finished WAL extension.

The conservative move is therefore to ship v1.5 with **honest XA**
that is in-process only, and queue the full crash-durable XA work for
v2.0.

## What Sprint 3 actually changed

Three commits, all under `crates/noxu-xa/` plus the user-facing
chapter:

1. `feat(xa): add XaError::CrashDurabilityNotSupported`
   New typed error variant with rustdoc documenting the v1.5
   limitation.

2. `fix(xa): xa_commit/xa_rollback honest after restart`
   When `xa_commit` or `xa_rollback` is called with an XID that
   exists in the persistent `_xa_prepared` log but not in the live
   `branches` map, we return `CrashDurabilityNotSupported` instead of
   the silent `NotFound`. `xa_recover` continues to surface the XID
   so operators can discover the in-doubt entry. `xa_forget` still
   works to clear the persistent record without resolving any data.
   Trait rustdoc on `xa_commit` / `xa_rollback` / `xa_recover` and on
   the `with_prepared_log` builder spell out the v1.5 limitation.

3. `fix(xa): xa_prepare auto-detects writes via inner Transaction`
   `xa_prepare` now consults `Txn::has_logged_entries()` (via
   `Transaction::get_inner_txn()`) to decide whether the branch
   performed writes. If the inner txn has logged anything, we go
   down the writes-present prepare path regardless of whether
   `mark_write` was called. `mark_write` is preserved as a no-op
   override and documented as no-longer-required for correctness.

Plus user-facing docs:

1. `docs(xa): document v1.5 in-process-only limitation`
   `docs/src/transactions/xa-distributed.md` now opens with a
   prominent admonition about the v1.5 limitation, drops the
   `mark_write` calls from the quick-start example, and replaces
   the cross-restart recovery example with one that demonstrates
   `xa_forget`.

## Regression coverage added

`crates/noxu-xa/tests/xa_v15_in_process_only_test.rs` (8 tests):

| Test | Asserts |
|---|---|
| `fresh_env_xa_recover_returns_empty_with_log` | Fresh env with prepared log: no in-doubt XIDs. |
| `fresh_env_xa_recover_returns_empty_without_log` | Same, no prepared log. |
| `xa_commit_after_restart_returns_crash_durability_not_supported` | The CRITICAL #1 fix \u2014 `xa_commit` after restart yields the typed error and the message mentions v1.5 + `xa_forget`. |
| `xa_rollback_after_restart_returns_crash_durability_not_supported` | Same for `xa_rollback`. |
| `xa_commit_unknown_xid_returns_not_found` | True unknown XID still surfaces as `NotFound`, not `CrashDurabilityNotSupported`. |
| `xa_forget_after_restart_clears_persistent_log` | Operators can clear the persistent log; subsequent `xa_recover` is empty and subsequent `xa_commit` yields `NotFound`. |
| `xa_prepare_auto_detects_writes_without_mark_write` | The Finding 3 fix \u2014 a write through the inner `Transaction` without an explicit `mark_write` is preserved through `xa_prepare` + `xa_commit`. |
| `xa_prepare_read_only_branch_still_returns_read_only` | Auto-detect must not produce a false positive on read-only workloads. |

Existing test suites (`xa_protocol_test`, `xa_adversarial_test`,
`xa_chaos_test`, the `noxu-xa` lib unit tests) all continue to pass
unmodified.

## v2.0 plan

To turn the v1.5 honest-but-restricted XA into a fully
crash-durable XA, the v2.0 work item is:

1. Add `LogEntryType::TxnPrepare` to `noxu-log`.
2. In `noxu-txn::Txn::commit_with_durability`, split out a
   `prepare()` step that emits a `TxnPrepare` WAL entry and leaves
   the txn in a new `Prepared` state with locks held.
3. In `noxu-recovery`, treat `TxnPrepare` as a "do not undo" marker
   on redo and emit recovered `(txn_id, xid)` pairs to the engine.
4. Add `XaEnvironment::recover_prepared_branches(env: &Environment)`
   (or hook into `with_prepared_log()`) that rebuilds the
   `branches` map from the recovery output, so post-restart
   `xa_commit` / `xa_rollback` work normally.
5. Remove `XaError::CrashDurabilityNotSupported` (or repurpose it
   for the configurations where the prepared log is not enabled).
6. Update `docs/src/transactions/xa-distributed.md` to drop the v1.5
   admonition and document the v2.0 cross-restart recovery flow.

The v1.5 commits are deliberately structured so the v2.0 work can
remove the new error variant cleanly: the in-memory state machine and
public API surface (other than the new error variant) are unchanged.

## References

* Audit: `docs/src/internal/api-audit-2026-05-persist-xa.md`
  (CRITICAL #1, CRITICAL #2, Finding 3).
* Implementation: `crates/noxu-xa/src/error.rs`,
  `crates/noxu-xa/src/environment.rs`,
  `crates/noxu-xa/src/resource.rs`.
* Regression tests: `crates/noxu-xa/tests/xa_v15_in_process_only_test.rs`.
* User-facing docs: `docs/src/transactions/xa-distributed.md`.
