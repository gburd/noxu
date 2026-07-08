# Concurrency Model

Noxu DB uses a **latch-based** concurrency model porting Noxu's approach.
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

```text
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

## Read-Path Structural Access

Structural access to buffers/pages (latches) is **orthogonal** to isolation
(record locks): a reader takes a cheap record read-lock for isolation, while
locating the bytes is a latch/atomic concern with no bearing on the isolation
model. The B-tree descent (`Tree::search`) is already lock-free-parallel via
hand-over-hand `read_arc()` shared guards, and a cache-resident LN is cloned
under the shared read guard.

When the cache is smaller than the working set, the evictor strips resident LN
payloads (keeping the slot + its LSN), so a read landing on a stripped slot
must re-fetch the LN from the log at that LSN
(`LogManager::read_entry`). That refill path is de-serialized so it does not
become a global chokepoint under many concurrent readers:

- **min-buffered-LSN skip.** A read whose LSN is older than any in-memory log
  buffer bypasses the global buffer-pool mutex entirely (consulting a lock-free
  `AtomicU64` mirror of the pool's oldest buffered LSN) and reads straight from
  disk/OS page cache.
- **Unparkable pin wait.** When a read must consult a buffer that still has an
  outstanding writer pin, it `futex_wait`s on the pin-count word and is woken
  the instant the writer's pin drops to zero (rather than a fixed timed park).
- **Settled-first scan.** The buffer scan checks zero-pin (settled) buffers
  before the active write buffer, so the common read never waits on the one
  buffer holding writer pins.

Repeat reads of a hot record are served from memory: after a cold refill the
BIN slot is re-populated with the fetched LN bytes (JE `IN.fetchTarget`), so
the next read hits the lock-free descend-and-clone path. Re-population is
guarded under the BIN write latch (it skips a slot whose LSN changed, an
already-populated slot, or a cursor-pinned BIN) and charges the same shared
memory counter the evictor credits on strip, keeping the cache bounded under
repeated read-then-evict cycles. None of this adds MVCC, versions, or
snapshots, and the WAL is not sharded.

## Thread Safety

| Handle | Safety |
|---|---|
| `Environment` | `Send + Sync` |
| `Database` | `Send + Sync` |
| `Transaction` | `Send` only — do not share across threads simultaneously |
| `Cursor` | `Send` only |
