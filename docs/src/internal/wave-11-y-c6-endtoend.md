# Wave 11-Y — C-6 End-to-End: NameLNTxn Inside the Creating Transaction

**Status**: Complete ✓

---

## Summary

Wave 11-Y closes the final gap in the C-6 recovery two-pass fix.
Wave 11-U made the undo _predicate_ functional (`run_mapping_tree_undo_pass`
checks aborted txns) but the predicate never fired for real WAL files because
Noxu still wrote the `NameLN` at commit time with `txn_id = None` — there was
nothing to undo.

This wave moves the write to **inside the creating transaction**
(`NameLNTxn`, `Provisional::Yes`) and removes the duplicate commit-time write.

| Item | Status | Files |
|------|--------|-------|
| C-6 end-to-end | ✓ Complete | `noxu-dbi/src/environment_impl.rs`, `noxu-db/src/environment.rs`, `noxu-recovery/src/recovery_manager.rs` |

---

## Root Cause (from Wave 11-U)

`EnvironmentImpl::commit_pending_database` wrote a `NameLN` WAL entry at
commit time with `txn_id = None`.  Because no `txn_id` was recorded, the
recovery scanner's `file_manager_scanner.rs` left `recovered_db_txn_ids`
empty for all databases, so `run_mapping_tree_undo_pass` had nothing to
remove — even when the creating transaction aborted.

---

## Fix

### `crates/noxu-dbi/src/environment_impl.rs`

1. **`open_database_transactional(name, config, txn_id: u64)`** — new
   parameter carries the creating transaction's ID.  (Breaking API change;
   only `noxu-db/src/environment.rs` calls this.)

2. **`open_database_inner(…, creating_txn_id: Option<u64>)`** — replaces the
   old `transactional_create: bool` parameter.  When `creating_txn_id` is
   `Some(txn_id)` and the database is new (not a recovery reopen), the method:
   - Inserts the name into `pending_names` (C-4 visibility guard unchanged).
   - Calls `log_name_ln_txn(lm, name, db_id, txn_id)` to write a
     `LogEntryType::NameLNTxn` entry with `txn_id = Some(txn_id as i64)` and
     `Provisional::Yes`.

3. **`commit_pending_database(name)`** — no longer calls `log_name_ln`.  The
   `NameLNTxn` was already written inside the transaction; the `TxnCommit`
   record written by the normal commit path is the durability marker.

4. **`log_name_ln_txn(lm, name, db_id, txn_id)`** — new helper mirror of
   `log_name_ln` that sets `txn_id = Some(…)`, `LogEntryType::NameLNTxn`,
   and `Provisional::Yes`.

### `crates/noxu-db/src/environment.rs`

The caller changed from a function-pointer dispatch to an explicit `if-else`:

```rust
if is_transactional_create {
    let txn_id = txn.expect("invariant").get_id();
    env_impl.open_database_transactional(name, &dbi_config, txn_id)
} else {
    env_impl.open_database(name, &dbi_config)
}
```

### `crates/noxu-recovery/src/recovery_manager.rs`

**`run_mapping_tree_undo_pass`** — updated predicate:

```rust
// OLD (explicit abort only):
analysis.aborted_txns.contains(&tid)

// NEW (aborted OR crash-before-commit):
!analysis.committed_txns.contains_key(&tid)
```

The old predicate missed the _crash-before-commit_ case: a transaction whose
`NameLNTxn` was flushed but which crashed before writing `TxnAbort` would be
in neither `committed_txns` nor `aborted_txns`.  The new predicate removes any
`NameLNTxn` entry whose `txn_id` is absent from `committed_txns`.

---

## Recovery Invariants

| WAL content | After recovery |
|---|---|
| `NameLN` (txn_id=None, old format or non-transactional) | **Kept** — treated as committed |
| `NameLNTxn` + `TxnCommit` | **Kept** — txn_id is in `committed_txns` |
| `NameLNTxn` + `TxnAbort` | **Removed** — txn_id not in `committed_txns` |
| `NameLNTxn` only (crash-before-commit) | **Removed** — txn_id not in `committed_txns` |

---

## Backward Compatibility

Old WAL files (pre-C6) contain `NameLN` entries with `txn_id = None`.
`file_manager_scanner.rs` maps `txn_id = None` → absent from
`recovered_db_txn_ids`.  The undo predicate returns `false` for entries absent
from `recovered_db_txn_ids` (the `unwrap_or(false)` arm), so old-format entries
always survive recovery.

See `docs/src/getting-started/migrating.md` for user-facing migration notes.

---

## Tests

All in `crates/noxu-recovery/src/recovery_manager.rs`:

| Test | Description |
|---|---|
| `test_c6_mapping_tree_undo_removes_aborted_namelns` | Updated unit test: covers committed, explicit-abort, no-txn (old-format), and crash-before-commit cases |
| `test_c6_aborted_db_creation_not_recovered` | **Un-ignored** end-to-end: NameLn(txn_id=42) + TxnAbort(42) → absent from recovered_db_names |
| `test_c6_committed_db_creation_is_recovered` | Regression guard: NameLn(txn_id=43) + TxnCommit(43) → present in recovered_db_names |
| `test_c6_old_format_namelns_always_recovered` | Old-log compat: NameLn(txn_id=None) → always present in recovered_db_names |

---

## Gate Results

- `cargo fmt --all -- --check`: ✓
- `cargo clippy --workspace --all-targets -- -D warnings`: ✓
- `RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps`: ✓
- `cargo test --workspace --no-fail-fast`: ✓ (all pass, 0 C-6 tests ignored)
- `make docs-check`: ✓
