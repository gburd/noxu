# Concurrency Model

Noxu DB uses a **latch-based** concurrency model porting JE's approach.
Two distinct levels of concurrency control apply:

1. **Latches** — short-duration mutual exclusion for in-memory structures
2. **Locks** — transaction-duration record-level locks for ACID isolation

## Latches

| Type | Rust type | Usage |
|------|-----------|-------|
| `ExclusiveLatch` | `parking_lot::Mutex<T>` | Single-writer access |
| `SharedLatch` | `parking_lot::RwLock<T>` | Tree nodes, shared structures |
| `AtomicU64` / `AtomicI64` | `std::sync::atomic` | Counters, flags, VLSNs |

### Latch Hierarchy (acquire outer before inner)

```
LogManager write latch
    └── TreeRoot latch (per-database)
            └── IN shared/exclusive latch
                    └── BIN write latch
                            └── LN (protected by record locks, no latch)
```

### Latch-Coupling Traversal

```rust
// Acquire child before releasing parent
let root_guard = tree.root.read();
let child_guard = root_guard.child[i].write();
drop(root_guard);  // now safe to release parent
```

## Record-Level Locks

Noxu DB uses **locking, not MVCC**. Writers hold locks until commit/abort;
readers block on write-locked records.

### Lock Compatibility Matrix

| Requester \ Holder | READ | WRITE | RANGE |
|---|---|---|---|
| READ | compat | conflict | conflict |
| WRITE | conflict | conflict | conflict |
| RANGE | conflict | conflict | conflict |

### Lock Manager

`LockManager` maintains a 64-bucket sharded lock table (sharded on
`hash(db_id, key)`). Each bucket is independently latched.

### Locker Hierarchy

| Locker | Use case |
|---|---|
| `BasicLocker` | Non-transactional auto-commit |
| `ThreadLocker` | Per-thread implicit locking (cursor reads) |
| `HandleLocker` | Cursor-lifetime locks |
| `Txn` | Explicit transactions — holds locks until commit/abort |

### Deadlock Detection

The waiter graph is maintained incrementally:
`waiter_graph: Mutex<HashMap<i64, Vec<i64>>>` (blocker→[waiters]).
`check_for_deadlock()` detects cycles; the youngest transaction is the victim
and receives `NoxuError::LockDeadlock`. Applications must abort and retry.

## Thread Safety

| Handle | Safety |
|---|---|
| `Environment` | `Send + Sync` |
| `Database` | `Send + Sync` |
| `Transaction` | `Send` only — do not share across threads simultaneously |
| `Cursor` | `Send` only |
