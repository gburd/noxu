# XA Distributed Transactions

> **Capability matrix:** see
> [Introduction → capability matrix](../introduction.md#capability-matrix).

Noxu DB supports the **X/Open XA** two-phase commit (2PC) protocol for
coordinating distributed transactions across multiple independent database
environments.  As of v2.0 (wave 3-2 of the v1.5+ remediation plan), XA is
**crash-durable**: a prepared branch survives a process crash and can be
resolved via `xa_recover` → `xa_commit` / `xa_rollback` after restart.

## Overview

The XA interface allows a **Transaction Manager** (TM) to coordinate
multiple **Resource Managers** (RMs).  Each RM manages one database
environment.  The TM drives the 2PC protocol:

1. **Phase 1 (Prepare)**: TM asks each RM to prepare.  If an RM can commit,
   it returns `Ok`; if it performed no writes, it returns `ReadOnly`.  A
   successful prepare writes a durable `TxnPrepare` WAL frame and fsyncs
   it before returning, so the prepared decision survives a crash.
2. **Phase 2 (Commit/Rollback)**: If all RMs prepared successfully, TM
   sends commit.  Otherwise, TM sends rollback.

If the process crashes between phases, the next `xa_recover()` call after
reopening the environment returns the in-doubt XIDs from the WAL so the TM
can resume the protocol.

## Quick Start

```rust,ignore
use noxu_xa::{XaEnvironment, XaResource, Xid, XaFlags, PrepareResult};
use noxu_db::{Environment, EnvironmentConfig, DatabaseConfig, DatabaseEntry};

// Open two environments (representing two clusters in the same process)
let env1 = Environment::open(config1).unwrap();
let env2 = Environment::open(config2).unwrap();
let xa1 = XaEnvironment::new(env1);
let xa2 = XaEnvironment::new(env2);

let db1 = xa1.inner().open_database(None, "accounts", &db_config).unwrap();
let db2 = xa2.inner().open_database(None, "ledger", &db_config).unwrap();

// Create a global transaction ID
let xid = Xid::new(1, b"transfer_001", b"branch_1").unwrap();

// Phase 0: Start branches on both RMs
xa1.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
xa2.xa_start(&xid, XaFlags::NOFLAGS).unwrap();

// Do work — mark_write is no longer required, writes are auto-detected.
let txn1 = xa1.get_transaction(&xid).unwrap();
db1.put(Some(txn1), &key, &debit_entry).unwrap();

let txn2 = xa2.get_transaction(&xid).unwrap();
db2.put(Some(txn2), &key, &credit_entry).unwrap();

// End branches
xa1.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
xa2.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();

// Phase 1: Prepare — durable WAL frame, fsync'd before each call returns.
let p1 = xa1.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
let p2 = xa2.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();

// Phase 2: Commit (only if both prepared Ok)
if p1 == PrepareResult::Ok { xa1.xa_commit(&xid, XaFlags::NOFLAGS).unwrap(); }
if p2 == PrepareResult::Ok { xa2.xa_commit(&xid, XaFlags::NOFLAGS).unwrap(); }
```

## Key Types

| Type | Description |
|------|-------------|
| `XaEnvironment` | Wraps a Noxu `Environment`; implements `XaResource` |
| `XaResource` | Trait defining the XA protocol operations |
| `Xid` | Transaction identifier: `format_id` + `global_transaction_id` (≤64 B) + `branch_qualifier` (≤64 B) |
| `XaFlags` | Operation flags: `NOFLAGS`, `JOIN`, `RESUME`, `TMSUCCESS`, `TMFAIL`, `TMSUSPEND`, `ONEPHASE` |
| `PrepareResult` | `Ok` (must commit/rollback) or `ReadOnly` (branch auto-cleaned) |
| `XaError` | Protocol errors: `NotFound`, `DuplicateXid`, `Protocol`, `Db`, etc. |

## State Machine

Each branch (identified by `Xid`) follows this state machine:

```text
          xa_start(NOFLAGS)
[none] ─────────────────────→ Active
                                │
              ┌─────────────────┼─────────────────┐
              │ xa_end(SUCCESS)  │ xa_end(SUSPEND) │ xa_end(FAIL)
              ▼                  ▼                  ▼
            Idle            Suspended         RollbackOnly
              │                  │                  │
              │  xa_start(RESUME)│                  │ xa_rollback
              │                  ▼                  ▼
              │              Active             [removed]
              │
    ┌─────────┼───────────┐
    │ prepare │ commit(1PC)│ rollback
    ▼         ▼            ▼
 Prepared  [committed]  [removed]
    │
    ├── xa_commit ──→ [committed]
    └── xa_rollback → [removed]
```

`Prepared` is the only branch state that survives a process crash (via
the durable `TxnPrepare` WAL frame).  All other states are in-memory
only; if the process exits while a branch is `Active`, `Suspended`,
`Idle`, or `RollbackOnly`, the branch is implicitly rolled back on the
next environment open (recovery undoes all writes from non-prepared,
non-committed transactions).

## Crash-durable contract (canonical pattern)

The defining feature of v2.0 XA is that `xa_prepare` is durable: a
prepared XID can be resolved across a process restart.

```rust,ignore
// Process A:
xa.xa_start(&xid, XaFlags::NOFLAGS)?;
// ... writes via xa.get_transaction(&xid) ...
xa.xa_end(&xid, XaFlags::TMSUCCESS)?;
xa.xa_prepare(&xid, XaFlags::NOFLAGS)?;       // <-- durable TxnPrepare frame written
//    *** PROCESS A CRASHES HERE ***

// Process B (after restart):
let env = Environment::open(config)?;
let xa  = XaEnvironment::new(env);            // <-- recovery surfaces in-doubt XIDs

let in_doubt = xa.xa_recover(XaFlags::STARTRSCAN)?;
assert!(in_doubt.contains(&xid));

// Decide based on the TM's own log:
xa.xa_commit(&xid, XaFlags::NOFLAGS)?;        // OR xa_rollback / xa_forget
// At this point the prepared writes are visible in the database AND
// durably committed to disk via a TxnCommit WAL frame.

let in_doubt = xa.xa_recover(XaFlags::STARTRSCAN)?;
assert!(in_doubt.is_empty());
```

The contract:

* `xa_prepare` does not return until the durable `TxnPrepare` frame is
  fsynced.
* If the process crashes between `xa_prepare` and resolution, the next
  `XaEnvironment::new(env)` (or `Environment::open(...)`) detects the
  in-doubt XIDs during recovery, and `xa_recover()` returns them.
* `xa_commit(xid)` on a recovered XID replays the prepared LNs into the
  in-memory tree AND writes a durable `TxnCommit` WAL frame.  After this
  call, subsequent `db.get()` operations see the committed values.
* `xa_rollback(xid)` on a recovered XID writes a durable `TxnAbort` WAL
  frame.  No tree work is needed: prepared writes are kept out of the
  in-memory tree during recovery, so there is nothing to undo.
* `xa_forget(xid)` is equivalent to `xa_rollback` for a recovered
  in-doubt branch (it writes a `TxnAbort` to durably resolve the WAL
  prepare frame).
* If the process crashes BEFORE the resolution call writes its
  `TxnCommit` / `TxnAbort` frame, the XID stays in-doubt across
  arbitrarily many further crashes until the TM finally resolves it.

## One-Phase Commit Optimization

When a transaction involves only a single RM, the TM can skip the prepare
phase and issue `xa_commit` with `XaFlags::ONEPHASE`:

```rust,ignore
xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
xa.xa_commit(&xid, XaFlags::ONEPHASE).unwrap(); // skips prepare
```

This saves one round-trip and is semantically equivalent to a normal
`Transaction::commit()`.  ONEPHASE commit has no effect on a recovered
in-doubt branch (the original process already wrote the prepare frame);
attempting it returns `XaError::Protocol`.

## Read-Only Optimization

If a branch performed no writes, `xa_prepare` returns
`PrepareResult::ReadOnly` and automatically cleans up the branch.  No
commit, rollback, or `TxnPrepare` WAL frame is written.

`xa_prepare` auto-detects whether the branch performed writes by
inspecting the inner `Transaction`'s log-entry chain.  You do **not**
need to call `mark_write` for correctness — a write performed through
the `Transaction` returned by `get_transaction(&xid)` is automatically
classified as a write.  `mark_write` is retained as a backwards-
compatible explicit override.

## Suspend/Resume

A branch can be suspended and later resumed, allowing a single thread to
interleave work across multiple branches:

```rust,ignore
xa.xa_start(&xid1, XaFlags::NOFLAGS).unwrap();
// ... work on xid1 ...
xa.xa_end(&xid1, XaFlags::TMSUSPEND).unwrap();

xa.xa_start(&xid2, XaFlags::NOFLAGS).unwrap();
// ... work on xid2 ...
xa.xa_end(&xid2, XaFlags::TMSUCCESS).unwrap();

// Resume xid1
xa.xa_start(&xid1, XaFlags::RESUME).unwrap();
// ... more work on xid1 ...
xa.xa_end(&xid1, XaFlags::TMSUCCESS).unwrap();
```

A `Suspended` branch is in-memory state only — if the process exits, the
branch is rolled back (recovery undoes its writes).  Only `Prepared`
survives a crash.

## Recovery — how it works

On the durable side:

1. `xa_prepare` writes a `TxnPrepare` WAL frame containing the txn id,
   the (first_lsn, last_lsn) range of the txn's logged LNs, and the
   encoded `Xid` (`format_id`, `gtrid`, `bqual`).  fsynced before
   returning.
2. `xa_commit` / `xa_rollback` write a `TxnCommit` / `TxnAbort` frame
   that closes out the prepare.
3. On recovery, `noxu-recovery` walks the WAL forward and tracks per-
   txn state:
   * `TxnCommit` / `TxnAbort` — txn is resolved; redo or undo as usual.
   * `TxnPrepare` followed by no resolution — txn is **in-doubt**.
     The recovery layer:
     * Does NOT undo the txn's writes (they may be committed via
       `xa_commit`).
     * Does NOT redo the txn's writes into the in-memory tree (they
       may be discarded via `xa_rollback`).
     * Records (xid, txn_id, first_lsn, last_lsn) and the LN payloads
       in a per-txn replay list.
4. After recovery, `Environment::recovered_prepared_txns()` returns the
   in-doubt list to the XA layer.
5. `XaEnvironment::new(env)` seeds an internal `recovered_branches` map
   from this list so `xa_recover()` can return the durable XIDs.
6. `xa_commit(xid)` on a recovered branch replays the LN list into the
   in-memory tree and writes a `TxnCommit` frame.  `xa_rollback(xid)`
   discards the LN list and writes a `TxnAbort` frame.

The optional `PreparedLog` database (`with_prepared_log()`) records each
prepared XID in a hidden `_xa_prepared` user database.  As of v2.0 the
WAL `TxnPrepare` frame is the source of truth for crash durability;
`PreparedLog` is retained as an operator-facing convenience for tools
that want to enumerate in-doubt XIDs without scanning the WAL.

### Heuristic completion

If the TM has lost its own log and cannot determine the outcome, use
`xa_forget` to discard an in-doubt branch:

```rust,ignore
xa.xa_forget(&xid, XaFlags::NOFLAGS).unwrap();
```

For an in-doubt branch (in-memory or recovered), `xa_forget` writes a
durable `TxnAbort` frame so the next recovery does not surface the XID
again.  Use with caution — any conditional logic the TM might have
applied to the prepared writes is lost.

## Error Handling

| Error | Meaning |
|-------|---------|
| `XaError::NotFound` | Unknown XID (XAER_NOTA) |
| `XaError::DuplicateXid` | `xa_start` with already-active XID, or with an XID that is in-doubt from recovery (XAER_DUPID) |
| `XaError::Protocol(msg)` | Operation called in wrong state (XAER_PROTO) |
| `XaError::Db(e)` | Underlying database error |

`XaError::CrashDurabilityNotSupported` is `#[deprecated]` and no longer
returned by the engine; it is retained as a public enum variant for
SemVer stability and will be removed in v3.0.

## Testing

The `noxu-xa` crate includes comprehensive test suites:

* **Unit tests**: cover each `XaResource` method individually.
* **Protocol tests**: deterministic coverage of every valid/invalid
  state transition, flag combination, and edge case.
* **Crash-durable tests** (`xa_crash_durable_test.rs`): the v2.0
  prepare → crash → recover → commit/rollback contract, including
  multiple in-doubt XIDs, double crashes before resolution, and
  durability across multiple reopens.
* **Chaos tests**: concurrent random XA operations across multiple
  clusters.
* **Scale tests**: 1000 concurrent branches, 8-thread contention.

Run with:

```bash
cargo test -p noxu-xa                                    # unit + protocol + scale
cargo test -p noxu-xa -- --ignored --nocapture           # + chaos + perf
XA_CHAOS_SECS=60 cargo test -p noxu-xa -- --ignored      # extended chaos
```
