# Wave 11-X: XA / Config / Cache-Budget Cross-Feature Fixes

**Branch**: `fix/wave11-x-config-xa-cluster`
**Target**: v3.0.0
**Status**: Complete

## Summary

Wave 11-X addresses four HIGH-severity cross-feature findings from the
second-pass cross-feature audit
(`docs/src/internal/audit-2026-05-2ndpass-crossfeature.md`):

| Finding | Severity | Area | Resolution |
|---------|----------|------|------------|
| X-11 | High | Config × WAL | `log_flush_no_sync_interval_ms` now consumed by `LogFlushTask` daemon |
| X-4 | High | XA × Recovery | Recovered-branch TOCTOU window closed with `resolving_xids` sentinel |
| X-10 | High | Txn Abort × Secondary × Cursor | Verified by test; locking already prevents torn state under READ\_COMMITTED |
| X-12 | High | Config × Memory | `cache_size` is now the total memory ceiling (Arbiter = `cache_size − log_buffers − off_heap`) |

## X-11: `log_flush_no_sync_interval_ms` now wired

**Problem**: `EnvironmentConfig::log_flush_no_sync_interval_ms` was stored
in `DbiEnvConfig` but never consumed. Users who set this parameter and used
`CommitNoSync` transactions received no background flush; data stayed in
write buffers indefinitely.

**Fix**: Added a `LogFlushTask` background daemon thread to `EnvironmentImpl`
(same pattern as the existing checkpointer/cleaner daemons). When
`log_flush_no_sync_interval_ms > 0`, the daemon wakes every N ms and calls
`LogManager::flush_no_sync()`, draining write buffers to the OS page cache
within the configured interval. When the interval is 0 (default), the daemon
thread exits immediately (disabled path, no overhead).

**Files changed**:

- `crates/noxu-dbi/src/environment_impl.rs`: `log_flush_no_sync_shutdown`,
  `log_flush_no_sync_handle` fields; daemon spawn in `new_with_config_inner`;
  shutdown in `close()`.
- `crates/noxu-dbi/tests/integration_tests.rs`: `test_x11_log_flush_no_sync_daemon_fires`,
  `test_x11_disabled_interval_no_spurious_flush`.

## X-4: Recovered XA branch TOCTOU

**Problem**: `xa_commit`'s recovered-branch path removed the XID from
`recovered_branches`, dropped the lock, then did I/O
(`apply_recovered_prepared_lns`, `write_txn_commit_for_recovered`). A
concurrent `xa_start(JOIN, xid)` during this window found the XID in neither
`recovered_branches` (already removed) nor `branches` (not yet inserted),
returning `XaError::NotFound` rather than the correct `XaError::Protocol`.
The same bug existed in `xa_rollback`'s recovered path.

**Fix**: Added `resolving_xids: Mutex<HashSet<Xid>>` to `XaEnvironment`.
Before dropping the `recovered_branches` lock, the XID is inserted into
`resolving_xids`. The lock is then released, I/O runs, and on completion
the XID is removed from `resolving_xids`. `xa_start(JOIN)` now checks both
`resolving_xids` and `recovered_branches` — if the XID is in either,
`XaError::Protocol` is returned (retryable) rather than `NotFound`.

**Locking order** (no deadlock): `recovered_branches` → `resolving_xids`
(in `xa_commit`/`xa_rollback`); `branches` → `resolving_xids` →
`recovered_branches` (in `xa_start`, each held briefly in sequence).

**Files changed**:

- `crates/noxu-xa/src/environment.rs`: `resolving_xids` field; `xa_start`
  JOIN guard; `xa_commit`/`xa_rollback` recovered paths; tests
  `test_xa4_join_on_recovered_xid_returns_protocol_not_notfound`,
  `test_xa4_join_active_branch_still_works`.

## X-10: Secondary index abort torn-state — verified, no code change

**Problem claimed by audit**: During `txn.abort()`, the undo loop reverts
primary and secondary records in reverse-LSN order. Between reverting the
secondary delete and reverting the primary write, a concurrent reader might
see "old secondary key → new primary value" (torn state).

**Investigation**: The abort undo loop (Phase 2 of `Transaction::abort`)
holds **write locks** on all modified slots (primary and secondary) for the
entire undo pass. Write locks are released in Phase 3 (`Txn::release_all_locks`)
**after** all undo has been applied. A concurrent READ\_COMMITTED reader that
acquires a read lock on any such slot blocks until Phase 3 completes and
sees a consistent before-image state. No code change is needed.

**Under READ\_UNCOMMITTED**: Dirty reads bypass locking, so the torn
intermediate state IS observable. This is expected behaviour for
READ\_UNCOMMITTED and is not a bug.

**Resolution**: Added regression test
`test_x10_secondary_abort_read_committed_no_torn_state` in
`crates/noxu-db/tests/secondary_decisions_test.rs` that runs 300
abort cycles with a concurrent secondary cursor under READ\_COMMITTED
and asserts no torn state is ever observed.

## X-12: `cache_size` is now the total memory budget

**Problem**: The `Arbiter` (BIN tree cache) was initialised with
`max_memory = cfg.cache_size`. The `LogManager` independently used
`cfg.log_buffer_size × cfg.log_num_buffers` bytes. The `OffHeapCache`
independently used `cfg.max_off_heap_memory` bytes. Total actual memory =
`cache_size + log_buffers + off_heap` — a user setting `cache_size = 1 GiB`
as the total ceiling would actually use more.

**Fix**: `cache_size` is now the total budget. The Arbiter is initialised
with:

```
arbiter_budget = max(cache_size − log_buf_total − off_heap_reserved, 1 MiB)
```

where `log_buf_total = log_num_buffers × log_buffer_size`. The floor at
1 MiB prevents the arbiter from being initialised with a non-positive budget
if the log+off-heap reservations exceed `cache_size`.

**Breaking change** (v3.0.0): Users who previously relied on
`cache_size` controlling only the BIN tree pool and sized
`log_buffer_size` / `max_off_heap_memory` independently will see the BIN
tree pool shrink by the size of those pools. See
`docs/src/getting-started/migrating.md` for the migration recipe.

**Files changed**:

- `crates/noxu-dbi/src/environment_impl.rs`: budget calculation;
  `get_arbiter_max_memory()` accessor.
- `crates/noxu-dbi/tests/integration_tests.rs`:
  `test_x12_arbiter_budget_subtracts_log_buffers`,
  `test_x12_arbiter_budget_subtracts_off_heap`.
- `docs/src/reference/configuration.md`: budget model documented.
- `docs/src/operations/sizing.md`: total-budget guidance added.

## Gate results

All 5807+ baseline tests pass plus the new regression tests for X-4, X-10,
X-11, X-12:

- `test_xa4_join_on_recovered_xid_returns_protocol_not_notfound` ✓
- `test_xa4_join_active_branch_still_works` ✓
- `test_x10_secondary_abort_read_committed_no_torn_state` ✓
- `test_x11_log_flush_no_sync_daemon_fires` ✓
- `test_x11_disabled_interval_no_spurious_flush` ✓
- `test_x12_arbiter_budget_subtracts_log_buffers` ✓
- `test_x12_arbiter_budget_subtracts_off_heap` ✓
