# Concurrency

Noxu DB supports multi-threaded applications with a flexible locking subsystem and
a robust mechanism for detecting and responding to lock conflicts.

## Thread Safety of Noxu Handles

The following table summarizes thread-safety characteristics of the main Noxu DB
types:

| Type | Thread Safety |
|------|--------------|
| `Environment` | Free-threaded (fully thread-safe) |
| `Database` | Free-threaded |
| `SecondaryDatabase` | Free-threaded |
| `Transaction` | Free-threaded |
| `Cursor` (transactional) | Usable from multiple threads if application serializes access to the handle |
| `Cursor` (non-transactional) | Must not be shared across threads |
| `SecondaryCursor` | Same conditions as `Cursor` |

Key terms:

- **Thread of control** — A thread performing Noxu DB operations.
- **Locking** — When a thread obtains exclusive or shared access to a record.
- **Free-threaded / thread-safe** — Can be shared across threads without explicit
  locking by the application.
- **Blocked** — A thread unable to obtain a lock because another thread holds a
  conflicting lock.
- **Deadlock** — Two or more threads blocked waiting on resources held by each
  other, making forward progress impossible without external intervention.
- **Lock conflict** — A thread failed to obtain a lock before the lock timeout
  expired. This may indicate a deadlock or a long-running operation. The response
  is the same in either case: abort and retry the transaction.

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
use noxu_db::{Environment, EnvironmentConfig, TransactionConfig};
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
let txn = env.begin_transaction(None, Some(&txn_config))?;

// Or set a timeout programmatically after the transaction starts.
let txn2 = env.begin_transaction(None, None)?;
txn2.set_lock_timeout(1000); // 1 second
```

---

# Read-Modify-Write Pattern

If you are reading a record for the purpose of modifying or deleting it, use the
**read-modify-write** (RMW) pattern. This causes Noxu DB to acquire a **write lock
at read time**, rather than acquiring a read lock and then upgrading it to a write
lock later.

Why this matters: upgrading a read lock to a write lock can cause deadlocks when
two transactions each hold a read lock on the same record and both try to upgrade
simultaneously. By taking the write lock immediately, you eliminate this class of
deadlock at the cost of reduced read concurrency.

> **Note:** RMW increases blocking because write locks are exclusive and cannot be
> shared. Use RMW only if you are seeing high rates of deadlocking.

```rust
use noxu_db::{DatabaseEntry, LockMode, NoxuError};

const MAX_RETRIES: u32 = 10;
let mut retries = 0;

loop {
    let txn = env.begin_transaction(None, None)?;

    let result = (|| -> Result<(), NoxuError> {
        let key = DatabaseEntry::from_bytes(b"counter");
        let mut data = DatabaseEntry::new();

        // Read with RMW: acquires a write lock immediately.
        db.get_with_lock_mode(
            Some(&txn),
            &key,
            &mut data,
            LockMode::Rmw,
        )?;

        // Modify the data in place.
        let current_value: u64 = data
            .get_data()
            .map(|b| u64::from_le_bytes(b.try_into().unwrap_or([0; 8])))
            .unwrap_or(0);
        let new_value = (current_value + 1).to_le_bytes();
        let new_data = DatabaseEntry::from_bytes(&new_value);

        // Write back. No special flag needed because we already hold the write lock.
        db.put(Some(&txn), &key, &new_data)?;
        txn.commit()?;
        Ok(())
    })();

    match result {
        Ok(()) => break,
        Err(NoxuError::LockConflict(_)) | Err(NoxuError::DeadlockDetected) => {
            let _ = txn.abort();
            retries += 1;
            if retries >= MAX_RETRIES {
                return Err("max retries exceeded".into());
            }
        }
        Err(e) => {
            let _ = txn.abort();
            return Err(e.into());
        }
    }
}
```

---
