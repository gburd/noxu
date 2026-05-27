# Wave 3-2 — Crash-durable XA two-phase commit

This note records the design and implementation of crash-durable XA
distributed transactions in Noxu DB v2.0.  It closes audit Critical
**C5** of `docs/src/internal/api-audit-2026-05-persist-xa.md` and
supersedes the v1.5 in-process-only restriction documented in
`docs/src/internal/sprint-3-xa-restriction.md`.

## Audit context

The May 2026 persist/XA API audit flagged C5 as a critical durability
defect:

> **C5: xa_prepare records the XID in a fsync'd PreparedLog but never
> tells the underlying noxu-db::Transaction.**  After a crash, recovery
> rolls the txn back unconditionally; xa_recover returns prepared XIDs
> but xa_commit fails with NotFound because the in-memory branches map
> is empty.  Two-phase commit non-functional across a crash.

Sprint 3A added a typed-error stop-gap
(`XaError::CrashDurabilityNotSupported`) and restricted XA to a single
process lifetime.  Wave 3-2 implements the underlying durability and
removes the restriction.

## Design

The implementation has five layers:

### 1. New WAL log entry: `TxnPrepare`

`crates/noxu-log/src/entry/txn_prepare_entry.rs` defines the new frame:

```text
  txn_id              : i64           (8 bytes)
  timestamp_ms        : u64           (8 bytes, ms since epoch)
  first_lsn           : u64           (8 bytes, first LN logged by this txn)
  last_lsn            : u64           (8 bytes, last LN logged before prepare)
  xid_format_id       : i32           (4 bytes)
  xid_gtrid_len       : u8            (1 byte, 0..=64)
  xid_bqual_len       : u8            (1 byte, 0..=64)
  xid_gtrid           : [u8; gtrid_len]
  xid_bqual           : [u8; bqual_len]
```

Fixed prefix is 38 bytes.  Variable suffix is 0..=128 bytes.  The
maximum gtrid/bqual lengths (64 each) match `noxu_xa::xid::MAXGTRIDSIZE`
and `MAXBQUALSIZE`, so well-formed `Xid` values always fit.

`LogEntryType::TxnPrepare = 32` was already declared in
`entry_type.rs`; wave 3-2 supplies the missing payload struct and
parser plus a unit-test suite.

**Breaking on-disk format change**: `LOG_VERSION` is bumped from `1` to
`2`.  v1 readers reject `TxnPrepare` frames as unknown-type errors;
v2 readers accept both formats.

### 2. `Txn::prepare` and `Transaction::prepare` API

`Txn` (the internal locker in `noxu-txn`) gains:

| Method                                    | Purpose |
|-------------------------------------------|---------|
| `prepare(format_id, gtrid, bqual)`        | Write TxnPrepare frame (with fsync), set `IS_PREPARED`. |
| `is_prepared()`                           | Flag accessor. |
| `clear_prepared_flag()`                   | Used by the outer `Transaction::resolved_*` helpers. |
| `resolved_commit_after_prepare()`         | Clear flag, run `commit_with_durability(CommitSync)`. |
| `resolved_abort_after_prepare()`          | Clear flag, run `abort()`. |

Direct `commit()` / `abort()` on a prepared `Txn` returns
`InvalidTransaction { state: "PREPARED" }`, blocking the protocol-error
path.  The previously unused `IS_PREPARED` constant (line 70 of
`txn.rs`, flagged by the audit) is now active.

`Transaction` (the public handle in `noxu-db`) wraps these:

| Method                                  | Purpose |
|-----------------------------------------|---------|
| `prepare(format_id, gtrid, bqual)`      | Write TxnPrepare via the outer's `LogManager`, flip inner flag, transition outer state to `Prepared`. |
| `resolved_commit_after_prepare()`       | Write TxnCommit via outer LM, run inner resolved-commit, transition to `Committed`. |
| `resolved_abort_after_prepare()`        | Write TxnAbort via outer LM, manually flip inner flag, run `abort_collect_undo`, apply undo to B-tree, release locks, transition to `Aborted`. |

A new `TransactionState::Prepared` variant blocks direct `commit()` /
`abort()` calls with a clear `OperationNotAllowed` message pointing
at the resolved paths.

Locks are **retained** across `prepare()`: a prepared transaction
holds every lock it acquired so concurrent readers cannot observe
in-flight state.  Locks are released only by
`resolved_commit_after_prepare` or `resolved_abort_after_prepare`.

### 3. Recovery integration

`crates/noxu-recovery/`:

* `LogEntry::TxnPrepare(TxnPrepareRecord)` is a new variant.
* `AnalysisResult` gains:
  * `prepared_txns: HashMap<u64, PreparedTxnInfo>` — keyed by txn id;
    each entry holds the XID, the prepare LSN, and the (first_lsn,
    last_lsn) range.
  * `record_prepare(info)` removes the txn from `active_txn_ids` and
    inserts into `prepared_txns`.
  * `record_commit` and `record_abort` REMOVE from `prepared_txns`,
    so a prepared+resolved txn (a normal full 2PC inside one process)
    is treated as cleanly committed/aborted.
  * `is_active()` excludes prepared txns.
* `RecoveryStats::prepared_txns` counts in-doubt frames.
* `run_undo` and `run_undo_all` SKIP prepared txns.  Redo also skips
  them automatically (only `is_committed` txns are redone).
* `collect_prepared_txn_lns` scans `redo_entries` and groups every LN
  whose `txn_id` belongs to a prepared txn into a
  `HashMap<u64, Vec<PreparedLnReplay>>`.  `PreparedLnReplay` is a
  deep-copy of the LN payload (`Vec<u8>` for key/data) so it outlives
  the WAL mmap region.
* `RecoveryInfo` gains `recovered_prepared_txns: Vec<PreparedTxnInfo>`
  and `prepared_txn_lns: HashMap<u64, Vec<PreparedLnReplay>>`,
  populated at the end of `recover()` and `recover_all()`.

`crates/noxu-dbi/`:

* `file_manager_scanner` parses TxnPrepare frames via
  `TxnPrepareEntry::read_from_log` and fills the entry's `lsn` field
  in the same way as `TxnCommit` and `CkptStart`.
* `EnvironmentImpl` captures `recovered_prepared_txns` and
  `recovered_prepared_lns` at recovery time and exposes them through
  three accessors: `recovered_prepared_txns()`,
  `take_recovered_prepared_lns(txn_id)`, and
  `forget_recovered_prepared_txn(txn_id)`.

### 4. `noxu-db` Environment plumbing

`Environment` exposes the recovered list and a small set of
durability helpers used by the XA layer:

| Method                                             | Purpose |
|----------------------------------------------------|---------|
| `recovered_prepared_txns()`                        | List of XA in-doubt prepared txns. |
| `take_recovered_prepared_lns(txn_id)`              | LN replay list (consumed at xa_commit). |
| `forget_recovered_prepared_txn(txn_id)`            | Drop on resolution. |
| `apply_recovered_prepared_lns(&[PreparedLnReplay])`| Apply replay list to in-memory tree. |
| `write_txn_commit_for_recovered(txn_id)`           | Durable TxnCommit frame for a recovered txn. |
| `write_txn_abort_for_recovered(txn_id)`            | Durable TxnAbort frame for a recovered txn. |

### 5. `noxu-xa::XaEnvironment`

`xa_prepare` now calls `Transaction::prepare(...)`, which writes the
durable `TxnPrepare` WAL frame.  The optional `PreparedLog` database
is retained as an operator convenience but is no longer the source of
truth for crash durability.

`XaEnvironment::new(env)` seeds an internal `recovered_branches:
HashMap<Xid, RecoveredBranch>` from `Environment::recovered_prepared_txns()`
so `xa_recover()` returns the in-doubt list even when no `PreparedLog`
is configured.

`xa_commit(xid)` looks first in the in-memory `branches` map, then
falls back to `recovered_branches`.  In the recovered path:

  1. Take the LN replay list with `take_recovered_prepared_lns`.
  2. Apply each LN to the in-memory tree via
     `apply_recovered_prepared_lns`.
  3. Write a durable `TxnCommit` frame via
     `write_txn_commit_for_recovered`.
  4. Drop the entry from the recovered list and the optional
     `PreparedLog` database.

`xa_rollback(xid)` mirrors this but discards the LN list (the prepared
writes were never applied during recovery, so there is nothing to
undo) and writes a `TxnAbort` frame.

`xa_forget(xid)` on an in-memory prepared branch now writes a
`TxnAbort` frame so the next recovery does not surface the still-
fsynced `TxnPrepare` again.

`XaError::CrashDurabilityNotSupported` is `#[deprecated]` and no
longer returned by the engine.

## Visibility timeline

```text
xa_start  xa_end  xa_prepare              xa_commit            (next reopen)
   |        |         |                      |                       |
   v        v         v                      v                       v
applied   applied   applied                applied                 applied
in-mem    in-mem    in-mem                  in-mem                  in-mem
tree      tree      tree                    tree                    tree
                    + TxnPrepare           + TxnCommit               (durable
                      WAL frame              WAL frame                via WAL)
                      (fsync'd)              (fsync'd)
```

For a recovered prepared txn (after the original process crashed):

```text
crash                   reopen + recover               xa_commit
  |                          |                            |
  v                          v                            v
TxnPrepare frame      writes NOT in tree              writes applied
in WAL (no resolution) (recovery skipped them)         to in-mem tree
                                                       + TxnCommit frame
```

## Test coverage

* **Unit tests** (`noxu-log`, `noxu-txn`):
  `TxnPrepareEntry` round-trip, truncated payload rejection,
  oversize-XID rejection, `Txn::prepare` writes the frame and retains
  locks, prepare blocks direct `commit()`/`abort()`,
  `resolved_commit_after_prepare`, `resolved_abort_after_prepare`,
  prepare-twice protocol error, prepare-after-commit protocol error,
  read-only prepare returns NULL_LSN.

* **Integration tests** (`noxu-xa`):
  * `xa_v15_in_process_only_test.rs` (rewritten): asserts the v2.0
    contract — `xa_commit_after_restart_succeeds`,
    `xa_rollback_after_restart_succeeds`,
    `xa_forget_after_restart_clears_persistent_log`, fresh-open
    xa_recover is empty, auto-detect-writes still works.
  * `xa_crash_durable_test.rs` (new, 8 tests): `prepare → crash →
    recover → commit/rollback`, two-XID mixed resolution, double
    crash before resolution preserves in-doubt status, negative
    protocol error tests, resolved-XID disappears from `xa_recover`.

## Audit findings closed

* **C5** — Crash-durable XA two-phase commit.  Closed by wave 3-2.

## References

* WAL frame: `crates/noxu-log/src/entry/txn_prepare_entry.rs`.
* Internal Txn API: `crates/noxu-txn/src/txn.rs::prepare`,
  `resolved_commit_after_prepare`, `resolved_abort_after_prepare`.
* Public Transaction API: `crates/noxu-db/src/transaction.rs::prepare`
  and resolved helpers.
* Recovery integration:
  `crates/noxu-recovery/src/{log_scanner,analysis_result,recovery_manager,recovery_info}.rs`.
* File-manager scanner: `crates/noxu-dbi/src/file_manager_scanner.rs`.
* Environment plumbing: `crates/noxu-dbi/src/environment_impl.rs`,
  `crates/noxu-db/src/environment.rs`.
* XA layer: `crates/noxu-xa/src/environment.rs`.
* Tests: `crates/noxu-xa/tests/xa_crash_durable_test.rs`,
  `crates/noxu-xa/tests/xa_v15_in_process_only_test.rs`.
* User-facing docs: `docs/src/transactions/xa-distributed.md`.
* Predecessor note: `docs/src/internal/sprint-3-xa-restriction.md`.
