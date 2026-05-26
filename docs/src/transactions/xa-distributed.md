# XA Distributed Transactions

> **v1.5 capability matrix:** see
> [Introduction → v1.5 capability matrix](../introduction.md#v15-capability-matrix).

Noxu DB supports the **X/Open XA** two-phase commit (2PC) protocol for
coordinating distributed transactions across multiple independent database
environments. This enables atomic commit/rollback spanning multiple Noxu
instances or other XA-capable resource managers.

## v1.5 limitation — in-process only

> **v1.5 XA is in-process only.** The persistent prepared log
> (`_xa_prepared`) is fsync’d by `xa_prepare`, but the engine does **not**
> currently emit a `TxnPrepare` WAL record and `noxu-recovery` does **not**
> reconstruct the in-memory `Transaction` for a prepared branch on a fresh
> process. Concretely:
>
> * If the process holding an `XaEnvironment` exits between `xa_prepare`
>   and `xa_commit` / `xa_rollback`, the branch’s in-memory state — write
>   locks, undo chain, dirty pages — is lost.
> * After restart, `xa_recover` still surfaces those XIDs (so an operator
>   can see what is in doubt), but `xa_commit` and `xa_rollback` of those
>   XIDs return `XaError::CrashDurabilityNotSupported`.
> * `xa_forget` continues to work, so the persistent record can be cleared
>   without resolving the underlying data.
>
> Cross-process / cross-restart XA recovery — a `TxnPrepare` log record and
> recovery integration — is planned for **v2.0**. See
> `docs/src/internal/sprint-3-xa-restriction.md` for the rationale.
>
> v1.5 XA is therefore appropriate for: distributed transactions whose
> coordinator and all participating Noxu environments share a single
> process lifetime (e.g. an application that embeds several `Environment`s
> and wants atomic commit across them). It is **not** appropriate for:
> standalone Resource Manager deployments expected to survive a TM crash
> in-doubt and resolve via `xa_recover` after restart.

## Overview

The XA interface allows a **Transaction Manager** (TM) to coordinate
multiple **Resource Managers** (RMs). Each RM manages one database
environment. The TM drives the 2PC protocol:

1. **Phase 1 (Prepare)**: TM asks each RM to prepare. If an RM can commit,
   it returns `Ok`; if it performed no writes, it returns `ReadOnly`.
2. **Phase 2 (Commit/Rollback)**: If all RMs prepared successfully, TM sends
   commit. Otherwise, TM sends rollback.

## Quick Start

```rust
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

// Phase 1: Prepare
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

## One-Phase Commit Optimization

When a transaction involves only a single RM, the TM can skip the prepare
phase and issue `xa_commit` with `XaFlags::ONEPHASE`:

```rust
xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
xa.xa_commit(&xid, XaFlags::ONEPHASE).unwrap(); // skips prepare
```

This saves one round-trip and is semantically equivalent to a normal
`Transaction::commit()`.

## Read-Only Optimization

If a branch performed no writes, `xa_prepare` returns
`PrepareResult::ReadOnly` and automatically cleans up the branch. No
commit or rollback is needed. This avoids unnecessary I/O for read-only
participants in a distributed transaction.

`xa_prepare` auto-detects whether the branch performed writes by
inspecting the inner `Transaction`’s log-entry chain. As of v1.5 you do
**not** need to call `mark_write` for correctness — a write performed
through the `Transaction` returned by `get_transaction(&xid)` is
automatically classified as a write. `mark_write` is retained as a
backwards-compatible explicit override.

## Suspend/Resume

A branch can be suspended and later resumed, allowing a single thread to
interleave work across multiple branches:

```rust
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

## Recovery (v1.5 — in-process only)

The `XaEnvironment` persists prepared XIDs in a dedicated internal database
(`_xa_prepared`, the **PreparedLog**). The persistent record survives
process crashes, but in v1.5 the engine does not write a `TxnPrepare` WAL
record and `noxu-recovery` does not reconstruct the in-memory
`Transaction` (locks, undo chain, dirty pages) for a prepared branch on a
fresh process.

In practice this means:

* Within a single process lifetime, `xa_recover` reports prepared branches
  and `xa_commit` / `xa_rollback` resolve them normally.
* Across a process restart, `xa_recover` still reports the persisted XIDs
  so an operator can see what is in doubt, but `xa_commit` /
  `xa_rollback` on those XIDs return
  `XaError::CrashDurabilityNotSupported`. The only safe operation is
  `xa_forget`, which clears the persistent record without touching any
  underlying data.

Crash-durable XA — prepared branches that survive a process restart and
can still be committed or rolled back — is planned for v2.0; see
`docs/src/internal/sprint-3-xa-restriction.md` and the linked tracking
issue.

### How it works

1. On `xa_prepare`: the XID is serialised and written to `_xa_prepared`
   as a durable record (format: `[format_id:4 LE][gtrid_len:1][gtrid][bqual]`).
2. On `xa_commit` / `xa_rollback` / `xa_forget`: the PreparedLog record is
   deleted.
3. On environment reopen: `xa_recover()` reads all surviving records —
   these are the in-doubt branches.

### Recovery workflow (single process)

```rust
// Within one process: prepared branches can be discovered and resolved.
let prepared_xids = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
for xid in &prepared_xids {
    // Decide: commit or rollback based on TM's persistent log
    xa.xa_commit(xid, XaFlags::NOFLAGS).unwrap();
}
assert!(xa.xa_recover(XaFlags::STARTRSCAN).unwrap().is_empty());
```

### After a process restart (v1.5)

```rust
// Reopen the environment after the previous process exited.
let env = Environment::open(config).unwrap();
let xa = XaEnvironment::new(env).with_prepared_log().unwrap();

// Discover in-doubt branches that were prepared by the previous process.
let prepared_xids = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
for xid in &prepared_xids {
    // v1.5: commit/rollback are NOT supported — the in-memory branch is
    // gone and v1.5 cannot rebuild it. xa_forget clears the persistent
    // record so xa_recover stops returning the XID.
    match xa.xa_commit(xid, XaFlags::NOFLAGS) {
        Err(noxu_xa::XaError::CrashDurabilityNotSupported) => {
            xa.xa_forget(xid, XaFlags::NOFLAGS).unwrap();
        }
        other => panic!("unexpected: {other:?}"),
    }
}
```

### Heuristic completion

If the TM has lost its own log and cannot determine the outcome (or, in
v1.5, the in-memory branch was lost across a restart), use `xa_forget`
to discard the in-doubt branch without committing or rolling back its
data:

```rust
xa.xa_forget(&xid, XaFlags::NOFLAGS).unwrap();
```

This removes the PreparedLog record but leaves any partially-committed
data in place — use with caution.

## Error Handling

| Error | Meaning |
|-------|---------|
| `XaError::NotFound` | Unknown XID (XAER_NOTA) |
| `XaError::DuplicateXid` | `xa_start` with already-active XID (XAER_DUPID) |
| `XaError::Protocol(msg)` | Operation called in wrong state (XAER_PROTO) |
| `XaError::CrashDurabilityNotSupported` | v1.5: XID exists only in the persistent prepared log; in-memory branch was lost on process restart — use `xa_forget`. |
| `XaError::Db(e)` | Underlying database error |

## Testing

The `noxu-xa` crate includes comprehensive test suites:

- **Unit tests** (16): cover each `XaResource` method individually
- **Protocol tests** (51): deterministic coverage of every valid/invalid state
  transition, flag combination, and edge case
- **Chaos tests**: concurrent random XA operations across multiple clusters
- **Scale tests**: 1000 concurrent branches, 8-thread contention
- **Performance tests**: 2PC vs 1PC vs plain transaction throughput comparison

Run with:
```bash
cargo test -p noxu-xa                                    # unit + protocol + scale
cargo test -p noxu-xa -- --ignored --nocapture           # + chaos + perf
XA_CHAOS_SECS=60 cargo test -p noxu-xa -- --ignored     # extended chaos
```
