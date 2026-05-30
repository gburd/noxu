# Deadlock Handling

Noxu DB uses record-level locking. When two transactions contend for the
same record in conflicting modes (one writing, one reading or writing), the
second transaction blocks until the first commits or aborts.

When a cycle is detected in the wait-for graph, Noxu DB selects a victim
transaction and returns `NoxuError::LockDeadlock` to it. The application
must abort and retry the victim transaction.

## Locks, Blocks, and Deadlocks

### Locks

Noxu DB uses a lock manager to provide transactional isolation. There are two
kinds of locks:

- **Write lock (exclusive)** — Granted when a transaction wants to write to a
  record. Prevents any other transaction from reading or writing that record.
- **Read lock (shared / non-exclusive)** — Granted for read-only access. Multiple
  transactions can hold read locks on the same record simultaneously. Prevents
  other transactions from writing the record while it is being read.

In a transactional application, the locker is the transaction handle. Locks are
held for the lifetime of the transaction: they are released when the transaction
commits or aborts.

### Blocks

A thread is blocked when it tries to obtain a lock on a record, but another thread
already holds a conflicting lock. The blocked thread cannot make forward progress
until the lock is granted or the operation is abandoned.

For example, if Transaction A holds a write lock on record 002, then Transaction B
trying to obtain a read or write lock on record 002 will be blocked. Similarly, if
Transactions A and B both hold read locks on record 002, and Transaction C tries to
obtain a write lock, Transaction C will be blocked until both A and B release their
read locks.

Blocking has performance implications. Strategies to reduce blocking:

- Keep transactions short so locks are held for less time.
- Access heavily-contended records near the end of a transaction.
- Use lower isolation levels (e.g., uncommitted reads or committed reads) where
  correctness allows. See [Isolation Levels](isolation.md).
- Arrange threads so they access records in the same order, reducing the chance of
  conflicting lock requests.

### Deadlocks

A deadlock occurs when two or more threads are each blocked waiting for a lock held
by the other. Neither thread can make progress.

Example: Thread A holds a write lock on record 001 and wants a write lock on
record 002. Thread B holds a write lock on record 002 and wants a write lock on
record 001. Both threads are deadlocked.

Noxu DB detects deadlocks and notifies your application by returning
`NoxuError::DeadlockDetected` or `NoxuError::LockConflict`. Your application must
respond by aborting the affected transaction and optionally retrying.

A **self-deadlock** can occur when two or more transactions within the same thread
are waiting on each other. Self-deadlocks cannot occur with one transaction per
thread, but you still must handle deadlocks with other threads.

To avoid deadlocks:

- Apply the same strategies used to avoid blocks.
- Ensure all threads access records in the same order. If threads lock records in
  the same basic order, deadlocks are impossible (blocking can still occur).
- When using secondary databases (indexes) expect deadlocks in concurrent
  applications, because locking order differs for reads and writes.

## Lock Management and Timeouts

The environment-level lock timeout specifies how long Noxu DB waits to acquire a
lock before returning a `LockConflict` error. You can configure this per
environment or per transaction:

```rust
use noxu::{Environment, EnvironmentConfig, TransactionConfig};
use std::path::PathBuf;

// Set a 500 ms lock timeout for the entire environment.
let env = Environment::open(
    EnvironmentConfig::new(PathBuf::from("/my/env/home"))
        .with_allow_create(true)
        .with_transactional(true)
        .with_lock_timeout(500),   // milliseconds
)?;

// Override the lock timeout for a single transaction.
let mut txn_config = TransactionConfig::new();
txn_config.set_no_wait(true); // Fail immediately if lock unavailable.
let txn = env.begin_transaction(Some(&txn_config))?;

// Or set a timeout programmatically after the transaction starts.
let txn2 = env.begin_transaction(None)?;
txn2.set_lock_timeout(1000); // 1 second
```

---
