# XA Distributed Transactions

Noxu DB supports the **X/Open XA** two-phase commit (2PC) protocol for
coordinating distributed transactions across multiple independent database
environments. This enables atomic commit/rollback spanning multiple Noxu
instances or other XA-capable resource managers.

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

// Open two environments (representing two clusters)
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

// Do work
let txn1 = xa1.get_transaction(&xid).unwrap();
db1.put(Some(txn1), &key, &debit_entry).unwrap();
xa1.mark_write(&xid).unwrap();

let txn2 = xa2.get_transaction(&xid).unwrap();
db2.put(Some(txn2), &key, &credit_entry).unwrap();
xa2.mark_write(&xid).unwrap();

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

If a branch performed no writes (no `mark_write()` calls), `xa_prepare`
returns `PrepareResult::ReadOnly` and automatically cleans up the branch.
No commit or rollback is needed. This avoids unnecessary I/O for read-only
participants in a distributed transaction.

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

## Recovery

After a crash, a TM can discover prepared-but-not-committed branches:

```rust
let prepared_xids = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
for xid in &prepared_xids {
    // Decide: commit or rollback based on TM's persistent log
    xa.xa_commit(xid, XaFlags::NOFLAGS).unwrap();
}
```

## Error Handling

| Error | Meaning |
|-------|---------|
| `XaError::NotFound` | Unknown XID (XAER_NOTA) |
| `XaError::DuplicateXid` | `xa_start` with already-active XID (XAER_DUPID) |
| `XaError::Protocol(msg)` | Operation called in wrong state (XAER_PROTO) |
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
