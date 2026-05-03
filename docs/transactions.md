# Getting Started with Noxu DB Transaction Processing

A guide for Rust developers on using transactions with Noxu DB, ported from the
Berkeley DB Java Edition Transaction Getting Started Guide.

---

## Table of Contents

1. [Introduction](#1-introduction)
   - [Transaction Benefits (ACID)](#transaction-benefits-acid)
   - [A Note on System Failure](#a-note-on-system-failure)
   - [Recoverability](#recoverability)
   - [Performance Tuning Overview](#performance-tuning-overview)
2. [Enabling Transactions](#2-enabling-transactions)
   - [Opening a Transactional Environment and Database](#opening-a-transactional-environment-and-database)
3. [Transaction Basics](#3-transaction-basics)
   - [Committing a Transaction](#committing-a-transaction)
   - [Aborting a Transaction](#aborting-a-transaction)
   - [Non-Durable Transactions](#non-durable-transactions)
   - [Auto-Commit](#auto-commit)
4. [Cursors and Transactions](#4-cursors-and-transactions)
5. [Secondary Indices with Transactions](#5-secondary-indices-with-transactions)
6. [Concurrency](#6-concurrency)
   - [Thread Safety of Noxu Handles](#thread-safety-of-noxu-handles)
   - [Locks, Blocks, and Deadlocks](#locks-blocks-and-deadlocks)
   - [Lock Management and Timeouts](#lock-management-and-timeouts)
7. [Isolation Levels](#7-isolation-levels)
   - [Supported Degrees of Isolation](#supported-degrees-of-isolation)
   - [Reading Uncommitted Data](#reading-uncommitted-data)
   - [Committed Reads](#committed-reads)
   - [Serializable Isolation](#serializable-isolation)
8. [Read-Modify-Write Pattern](#8-read-modify-write-pattern)
9. [Backup and Recovery](#9-backup-and-recovery)
   - [Normal Recovery](#normal-recovery)
   - [Performing Backups](#performing-backups)
10. [Performance Tuning](#10-performance-tuning)

---

## 1. Introduction

This guide provides a thorough introduction to transactions as used with Noxu DB,
a Rust port of Berkeley DB Java Edition (BDB JE 7.5.11). It covers the guarantees
that transactions provide, the application infrastructure required for full
transactional protection, and practical examples of writing transactional Rust code.

You should be familiar with the basic Noxu DB API — opening environments,
databases, and performing simple reads and writes — before reading this guide.

### Transaction Benefits (ACID)

Transactions protect your application's data from failures. Noxu DB transactions
provide full **ACID** guarantees:

**Atomicity**

Multiple database operations are treated as a single unit of work. Once committed,
all writes performed under the protection of the transaction are saved. If a
transaction is aborted, all writes are discarded and the database is left in the
state it was in before the transaction began, regardless of the number or type of
write operations performed. A single transaction may span multiple database handles
within the same environment.

**Consistency**

Your databases will never contain a partially completed transaction. This is true
even if your application fails while transactions are in progress. If the
application or OS fails, either all changes appear when the application restarts,
or none of them do.

**Isolation**

While a transaction is in progress, the database appears to that transaction as if
no other operations are occurring outside of it. Operations wrapped inside a
transaction always have a clean and consistent view and never see updates in
progress under another transaction. Isolation guarantees can be relaxed for
performance; see [Isolation Levels](#7-isolation-levels).

**Durability**

Once committed, modifications persist even in the event of an application or OS
failure. Like isolation, the durability guarantee can be relaxed; see
[Non-Durable Transactions](#non-durable-transactions).

### A Note on System Failure

Throughout this guide, "protection against system or application failure" means
protection against the most likely culprits for crashes. As long as your data
modifications have been committed to disk, those modifications should persist even
if your application or OS subsequently fails. Even if the application fails in the
middle of a commit or abort, the data on disk will be in a consistent state or
enough information will be present to bring the database into a consistent state
via recovery.

> **Note on disk write caches:** Many disks have a write cache that may be enabled
> by default. A transaction can appear committed to your application while the data
> still resides only in the write cache. If the disk write cache has no battery
> backup, data can be lost after an OS crash even with maximum durability mode. For
> maximum durability, disable the disk write cache or use one with battery backup.

Disk failure is outside the scope of transactional protection; the benefit of
transactions is only as good as the backups you have taken. No API can protect
against logic failures in your own code — transactions cannot protect you from
writing the wrong data to your databases.

### Recoverability

Durability means that once a transaction is committed, the database modifications
performed under its protection will not be lost due to system failure.

Noxu DB runs a normal recovery against a subset of its log files every time an
environment is opened. This is a routine procedure that ensures the database is in
a consistent state on startup. It is performed automatically and requires no
application intervention.

Noxu DB also supports archival backup and recovery in the case of catastrophic
failure such as the loss of a physical disk drive. See
[Backup and Recovery](#9-backup-and-recovery).

### Performance Tuning Overview

The use of transactions is not free. Transaction commits usually require disk I/O
that non-transactional applications do not perform. For multi-threaded applications,
transactions can increase lock contention due to extra locking required by
transactional isolation guarantees. Performance tuning considerations are discussed
throughout this guide and are summarized in [Performance Tuning](#10-performance-tuning).

---

## 2. Enabling Transactions

To use transactions you must:

1. Enable transactions on the environment using `EnvironmentConfig::with_transactional(true)`.
2. Open the environment before opening any databases.
3. Open databases using the same environment handle. Common practice is to use
   auto-commit for the database open (pass `None` for the transaction handle), so
   the open itself is automatically transaction-protected.

### Opening a Transactional Environment and Database

```rust
use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let home = PathBuf::from("/my/env/home");

    // Open a transactional environment, creating it if it does not exist.
    let env_config = EnvironmentConfig::new(home)
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_config)?;

    // Open the database. Passing None for the transaction causes the open
    // to be protected by auto-commit.
    let db_config = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true);
    let db = env.open_database(None, "sampleDatabase", &db_config)?;

    // ... use the database ...

    db.close()?;
    env.close()?;
    Ok(())
}
```

> **Warning:** Never close a database that has active transactions. Make sure all
> transactions are resolved (either committed or aborted) before closing the
> database or the environment.

---

## 3. Transaction Basics

Once you have a transactional environment and database open, you protect operations
by acquiring a transaction handle and passing it to the database methods you want
to include in that transaction.

```rust
use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus,
};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let home = PathBuf::from("/my/env/home");

    let env = Environment::open(
        EnvironmentConfig::new(home)
            .with_allow_create(true)
            .with_transactional(true),
    )?;

    let db = env.open_database(
        None,
        "sampleDatabase",
        &DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true),
    )?;

    let key = DatabaseEntry::from_bytes(b"thekey");
    let data = DatabaseEntry::from_bytes(b"thedata");

    // Begin a transaction.
    let txn = env.begin_transaction(None, None)?;

    // Perform the write under the transaction.
    match db.put(Some(&txn), &key, &data) {
        Ok(_) => {
            txn.commit()?;
            println!("Write committed.");
        }
        Err(e) => {
            txn.abort()?;
            println!("Write failed, transaction aborted: {}", e);
        }
    }

    db.close()?;
    env.close()?;
    Ok(())
}
```

Key rules:
- Obtain a transaction with `env.begin_transaction(parent, config)`. Pass `None`
  for the parent unless you want a nested (child) transaction.
- Pass `Some(&txn)` as the first argument to `db.put()`, `db.get()`,
  `db.delete()`, and `db.open_cursor()`.
- Commit with `txn.commit()` or roll back with `txn.abort()`.
- Once committed or aborted, a transaction handle must not be used again. Any
  further calls will return an error.
- All transaction handles must be committed or aborted before closing databases
  and the environment.

### Committing a Transaction

When you commit a transaction, the following occurs:

1. A commit record is written to the log, indicating that the modifications are
   now permanent. By default this write is performed synchronously to disk so the
   commit record arrives in the log files before any other action is taken.
2. Any log information held in memory is, by default, synchronously flushed to
   disk. This requirement can be relaxed; see [Non-Durable Transactions](#non-durable-transactions).
3. All locks held by the transaction are released, making the modifications
   visible to other transactions or threads.

Only the B-tree leaf nodes modified by a transaction are written to the log files
on commit. Other internal B-tree structures are left unwritten. Over time this
means recovery time can increase if no checkpoints are run. The background
checkpointer thread runs periodically by default to minimize recovery time.

```rust
txn.commit()?;
// The handle is now invalid; do not use txn again.
```

### Aborting a Transaction

Aborting a transaction discards all database writes made under its protection. The
database is left in the state it was in before the transaction began.

```rust
let txn = env.begin_transaction(None, None)?;

// ... perform some operations ...

// Something went wrong — roll everything back.
txn.abort()?;
// The handle is now invalid.
```

A typical write-retry loop:

```rust
use noxu_db::{DatabaseEntry, NoxuError};

const MAX_RETRIES: u32 = 10;
let mut retries = 0;

loop {
    let txn = env.begin_transaction(None, None)?;

    let result = (|| -> Result<(), NoxuError> {
        let key = DatabaseEntry::from_bytes(b"mykey");
        let data = DatabaseEntry::from_bytes(b"myvalue");
        db.put(Some(&txn), &key, &data)?;
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
            // Back off slightly before retrying (optional).
        }
        Err(e) => {
            let _ = txn.abort();
            return Err(e.into());
        }
    }
}
```

### Non-Durable Transactions

By default, transaction commits are durable because Noxu DB synchronously writes
and flushes the log to disk. You can relax this for performance by configuring a
different `Durability` policy.

Noxu DB provides three sync policies via `SyncPolicy`:

| Policy | Description | Durability |
|--------|-------------|------------|
| `SyncPolicy::Sync` | Write and fsync on commit (default) | Maximum — survives OS crash |
| `SyncPolicy::WriteNoSync` | Write to OS buffers, no fsync | Survives app/JVM crash, not OS crash |
| `SyncPolicy::NoSync` | No write or fsync on commit | Minimum — may lose data on app crash |

You can set the default durability on the environment, or override it
per-transaction using `TransactionConfig`:

```rust
use noxu_db::{
    DatabaseConfig, DatabaseEntry, Durability, Environment, EnvironmentConfig,
    SyncPolicy, ReplicaAckPolicy, TransactionConfig,
};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set a default durability of WRITE_NO_SYNC for the entire environment.
    let no_sync = Durability::new(
        SyncPolicy::WriteNoSync,
        SyncPolicy::WriteNoSync,      // replica sync (unused for standalone)
        ReplicaAckPolicy::SimpleMajority, // replica ack (unused for standalone)
    );

    let env = Environment::open(
        EnvironmentConfig::new(PathBuf::from("/my/env/home"))
            .with_allow_create(true)
            .with_transactional(true)
            .with_durability(no_sync),
    )?;

    let db = env.open_database(
        None,
        "sampleDatabase",
        &DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true),
    )?;

    // Override durability for a specific transaction using NO_SYNC.
    let txn_config = TransactionConfig::new()
        .with_durability(Durability::COMMIT_NO_SYNC);
    let txn = env.begin_transaction(None, Some(&txn_config))?;

    let key = DatabaseEntry::from_bytes(b"thekey");
    let data = DatabaseEntry::from_bytes(b"thedata");

    match db.put(Some(&txn), &key, &data) {
        Ok(_) => txn.commit()?,
        Err(e) => {
            txn.abort()?;
            return Err(e.into());
        }
    }

    db.close()?;
    env.close()?;
    Ok(())
}
```

The three named constants on `Durability` cover the most common cases:

```rust
use noxu_db::Durability;

// Equivalent to SyncPolicy::Sync (the default): maximum durability.
let _d = Durability::COMMIT_SYNC;

// Write to OS buffers, no fsync: good balance of performance and safety.
let _d = Durability::COMMIT_WRITE_NO_SYNC;

// No write or fsync: maximum performance, minimum durability.
let _d = Durability::COMMIT_NO_SYNC;
```

### Auto-Commit

While transactions are frequently used to group multiple operations atomically,
sometimes you only need to protect a single write. Rather than explicitly creating
a transaction, committing, and handling errors, you can use **auto-commit** by
passing `None` as the transaction argument to a write operation on a transactional
database.

```rust
use noxu_db::{DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env = Environment::open(
        EnvironmentConfig::new(PathBuf::from("/my/env/home"))
            .with_allow_create(true)
            .with_transactional(true),
    )?;

    let db = env.open_database(
        None,
        "sampleDatabase",
        &DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true),
    )?;

    let key = DatabaseEntry::from_bytes(b"thekey");
    let data = DatabaseEntry::from_bytes(b"thedata");

    // Passing None causes this write to be automatically wrapped in
    // its own transaction and committed.
    db.put(None, &key, &data)?;

    db.close()?;
    env.close()?;
    Ok(())
}
```

> **Note:** Auto-commit is not available for cursors. You must always open a
> cursor with an explicit transaction handle if you want its operations to be
> transaction-protected.

> **Warning:** Never have more than one active transaction in your thread at a
> time. Mixing an explicit transaction with an auto-commit operation in the same
> thread can result in undetectable deadlocks.

---

## 4. Cursors and Transactions

You can protect cursor operations by opening the cursor with a transaction handle.
After that, you do not provide a transaction handle directly to cursor methods —
all subsequent cursor operations automatically participate in the transaction.

**You must close the cursor before committing or aborting the transaction.**

```rust
use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    LockMode, OperationStatus,
};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env = Environment::open(
        EnvironmentConfig::new(PathBuf::from("/my/env/home"))
            .with_allow_create(true)
            .with_transactional(true),
    )?;

    let db = env.open_database(
        None,
        "sampleDatabase",
        &DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true),
    )?;

    let replacement = b"new data";

    let txn = env.begin_transaction(None, None)?;

    // Open the cursor with the transaction handle.
    let mut cursor = db.open_cursor(Some(&txn), None)?;

    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();

    let result = (|| -> Result<(), noxu_db::NoxuError> {
        loop {
            let status = cursor.get(
                &mut key,
                &mut data,
                Get::Next,
                Some(LockMode::Default),
            )?;
            if status != OperationStatus::Success {
                break;
            }
            // Replace the current record's data.
            let new_data = DatabaseEntry::from_bytes(replacement);
            cursor.put_current(&new_data)?;
        }
        // Close the cursor BEFORE committing.
        cursor.close()?;
        txn.commit()?;
        Ok(())
    })();

    if let Err(e) = result {
        // cursor may already be closed; ignore errors here
        let _ = txn.abort();
        return Err(e.into());
    }

    db.close()?;
    env.close()?;
    Ok(())
}
```

If you need to iterate in a concurrent application and want to allow other writers
to proceed, consider using a lower isolation level for the cursor (see
[Isolation Levels](#7-isolation-levels)).

---

## 5. Secondary Indices with Transactions

You can use transactions with secondary databases as long as you open the secondary
database with `with_transactional(true)` in its `SecondaryConfig`. All other
aspects of using secondary indices with transactions are identical to using them
without transactions.

Protect secondary cursors the same way as primary cursors: open the cursor with a
transaction handle, and close the cursor before committing or aborting.

When you use transactions to protect writes, primary and secondary indices are
updated atomically within the same transaction, preventing secondary index
corruption.

```rust
use noxu_db::{
    DatabaseConfig, Environment, EnvironmentConfig, SecondaryConfig,
    SecondaryDatabase,
};
use std::path::PathBuf;

fn open_secondary_transactional(
    env: &Environment,
    primary: &noxu_db::Database,
) -> Result<SecondaryDatabase, Box<dyn std::error::Error>> {
    let sec_config = SecondaryConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_key_creator(Box::new(my_key_creator));

    // Passing None for the transaction causes the open to use auto-commit.
    let sec_db = env.open_secondary(
        None,
        "mySecondaryDatabase",
        None,
        primary,
        &sec_config,
    )?;

    Ok(sec_db)
}
# fn my_key_creator(_: &noxu_db::DatabaseEntry, _: &noxu_db::DatabaseEntry,
#     _: &mut noxu_db::DatabaseEntry) -> bool { false }
```

> **Note:** If you use a secondary index and you are writing a concurrent
> application, expect deadlocks. The lock ordering for reads and writes on
> secondary databases differs from that of primary databases, making deadlocks more
> likely. Always write deadlock-retry logic (see the retry loop in
> [Aborting a Transaction](#aborting-a-transaction)).

---

## 6. Concurrency

Noxu DB supports multi-threaded applications with a flexible locking subsystem and
a robust mechanism for detecting and responding to lock conflicts.

### Thread Safety of Noxu Handles

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

### Locks, Blocks, and Deadlocks

**Locks**

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

**Blocks**

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
  correctness allows. See [Isolation Levels](#7-isolation-levels).
- Arrange threads so they access records in the same order, reducing the chance of
  conflicting lock requests.

**Deadlocks**

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

### Lock Management and Timeouts

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

## 7. Isolation Levels

Isolation guarantees are a critical part of transactional protection. The stronger
the isolation, the more locking is required, which increases the chance of blocking
and reduces throughput. Relaxing isolation can improve performance but exposes your
application to anomalies.

### Supported Degrees of Isolation

| Degree | ANSI Term | Noxu Behavior |
|--------|-----------|---------------|
| 1 | READ UNCOMMITTED | Reads may see data modified but not yet committed by another transaction (dirty reads). A transaction may read data that is subsequently rolled back and never existed in the database. |
| 2 | READ COMMITTED | Dirty reads are prevented. Read locks are released as soon as the cursor moves past a record, rather than being held for the life of the transaction. Data at the current cursor position will not change, but previously read data can change after the cursor moves. |
| (default) | REPEATABLE READ | Read and write locks are held until the transaction completes. Data read by a transaction will not be modified by another transaction before the reading transaction completes. **This is the Noxu DB default.** |
| 3 | SERIALIZABLE | Repeatable read is observed plus no phantom reads. Phantoms are records that appear in a search result on a second execution that were absent on the first. Noxu DB prevents phantoms with additional range locking. |

By default, Noxu DB transactions use repeatable read isolation. You can configure
a lower level (uncommitted read, committed read) for performance or a higher level
(serializable) for correctness when phantoms are a concern.

### Reading Uncommitted Data

Uncommitted reads (dirty reads) allow one transaction to see data that has been
modified but not yet committed by another. This can improve performance by
eliminating read locks, but the data you read may subsequently be rolled back.

Configure uncommitted reads at the transaction level:

```rust
use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    TransactionConfig,
};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env = Environment::open(
        EnvironmentConfig::new(PathBuf::from("/my/env/home"))
            .with_allow_create(true)
            .with_transactional(true),
    )?;

    let db = env.open_database(
        None,
        "sampleDatabase",
        &DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true),
    )?;

    // Use uncommitted reads for this transaction.
    let txn_config = TransactionConfig::new().with_read_uncommitted(true);
    let txn = env.begin_transaction(None, Some(&txn_config))?;

    let key = DatabaseEntry::from_bytes(b"thekey");
    let mut data = DatabaseEntry::new();
    db.get(Some(&txn), &key, &mut data)?;

    txn.commit()?;
    db.close()?;
    env.close()?;
    Ok(())
}
```

You can also request uncommitted reads on a per-operation basis using
`LockMode::ReadUncommitted`:

```rust
use noxu_db::{DatabaseEntry, LockMode};

// (env, db, txn assumed to be open)
let key = DatabaseEntry::from_bytes(b"thekey");
let mut data = DatabaseEntry::new();

// Pass the lock mode directly to the get call.
db.get_with_lock_mode(Some(&txn), &key, &mut data, LockMode::ReadUncommitted)?;
```

### Committed Reads

Read committed isolation means read locks are released as soon as the cursor
advances past a record, rather than being held for the transaction's lifetime. This
allows other transactions to modify records that have already been read and moved
past, potentially improving throughput when you are scanning forward through data
and do not need to re-read previous records.

```rust
use noxu_db::TransactionConfig;

// Use read committed isolation for this transaction.
let txn_config = TransactionConfig::new().with_read_committed(true);
let txn = env.begin_transaction(None, Some(&txn_config))?;
```

Or per-operation:

```rust
use noxu_db::LockMode;

db.get_with_lock_mode(Some(&txn), &key, &mut data, LockMode::ReadCommitted)?;
```

Read committed is most useful for forward-scanning cursors that never need to
re-read previously visited records.

### Serializable Isolation

Serializable isolation prevents **phantom reads**: queries that return different
results when executed a second time within the same transaction because another
transaction inserted or deleted matching records in between.

Under repeatable read (the default), a transaction T can perform a search that
returns `NotFound`, and then the same search can return `Success` later in the same
transaction if another transaction inserted a matching record. With serializable
isolation, this cannot happen.

Serializable isolation causes additional locking (range locks) which can reduce
concurrency. Use it only when your application requires it.

Configure serializable isolation environment-wide by setting
`with_txn_serializable_isolation(true)` on `EnvironmentConfig`:

```rust
use noxu_db::{Environment, EnvironmentConfig};
use std::path::PathBuf;

let env = Environment::open(
    EnvironmentConfig::new(PathBuf::from("/my/env/home"))
        .with_allow_create(true)
        .with_transactional(true)
        .with_txn_serializable_isolation(true),
)?;
```

Or configure serializable isolation for a single transaction:

```rust
use noxu_db::TransactionConfig;

// Serializable isolation is achieved by combining serializable flag
// with the transaction config.
let txn_config = TransactionConfig::new()
    .with_serializable_isolation(true);
let txn = env.begin_transaction(None, Some(&txn_config))?;
```

---

## 8. Read-Modify-Write Pattern

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

## 9. Backup and Recovery

### Normal Recovery

Noxu DB organizes its data as a B-tree, and all write operations are logged to
`.ndb` log files on disk. When database records are created, modified, or deleted,
the modifications are represented in the B-tree's leaf nodes. On a transactional
commit, only the leaf nodes modified by the transaction are written to the log.

**Normal recovery** is the process of reconstructing the complete B-tree from the
leaf-node information in the log files. This is run automatically every time a Noxu
DB environment is opened; no application action is required. The checkpointer
background thread runs periodically to write a complete, consistent checkpoint to
disk, which reduces the amount of log that must be replayed on the next recovery
and thus shortens startup time.

If an `EnvironmentFailure` error is returned, call `env.is_valid()`:
- If it returns `true`, you can continue using the environment.
- If it returns `false`, close and reopen all `Environment` handles so that normal
  recovery runs.

### Performing Backups

The fundamental backup operation is to copy Noxu DB log files (`.ndb` files) to
safe storage. To restore, copy the files back to the environment directory and
reopen the environment; normal recovery reconstructs the B-tree automatically.

**Hot Backup (Online)**

A hot backup is taken while write operations are in progress. Copy all `.ndb` log
files from the environment directory to your archival location. Files must be
copied in alphabetical (numerical) order. You do not need to stop database
operations.

The complication with hot backups is that the log cleaner may delete or create
files while you are copying. A naive copy loop may miss newly created files. The
recommended solution is to do two passes:

1. Enumerate all log files and begin copying.
2. After finishing, check for any new files created during the copy and copy those
   as well.

Or use a systematic approach:

```rust
use std::fs;
use std::path::{Path, PathBuf};

/// Copy all .ndb log files from `env_dir` to `backup_dir` in order.
/// A simple hot-backup approach; for production use, implement two-pass
/// logic or freeze the log file set before copying.
fn hot_backup(env_dir: &Path, backup_dir: &Path) -> std::io::Result<()> {
    fs::create_dir_all(backup_dir)?;

    let mut log_files: Vec<PathBuf> = fs::read_dir(env_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().map(|x| x == "ndb").unwrap_or(false))
        .collect();

    // Sort numerically (alphabetical order matches numerical for hex-named files).
    log_files.sort();

    for file in &log_files {
        let dest = backup_dir.join(file.file_name().unwrap());
        fs::copy(file, dest)?;
        println!("Backed up {:?}", file.file_name().unwrap());
    }

    Ok(())
}
```

**Offline Backup**

An offline backup guarantees you capture the database including all in-memory cache
contents at the moment of the backup:

1. Stop all write operations on the database.
2. Ensure all in-memory changes are flushed to disk:
   - If using durable transactions (the default `SyncPolicy::Sync`), simply make
     sure all in-progress transactions are committed or aborted.
   - If using non-durable transactions, run a checkpoint, or close the environment
     (which runs a checkpoint automatically).
3. Optionally run a checkpoint to shorten future recovery time.
4. Copy all `.ndb` log files to the archival location.
5. Resume normal operations.

**Incremental Backups**

An incremental backup copies only those log files modified or created since the
last backup. Track the last log file number included in each backup and on the next
run copy only files with higher numbers. Most system backup tools support
incremental backup natively.

**Restore**

To restore from backup:
1. Copy the backed-up `.ndb` log files to the environment directory.
2. Open the environment normally. Normal recovery will reconstruct the B-tree.

For catastrophic recovery (e.g., after a disk failure), restore from the most
recent full backup and then apply any subsequent incremental backups in order
before opening the environment.

---

## 10. Performance Tuning

The use of transactions introduces overhead that non-transactional applications
do not incur. Key tuning considerations:

**Durability vs. Throughput**

The default `SyncPolicy::Sync` performs a full fsync on every commit. This is the
most durable option but also the slowest. If you can tolerate the possibility of
losing the most recent committed transactions in the event of an OS crash, use
`SyncPolicy::WriteNoSync`. If you can tolerate losing committed transactions on
application crash as well, use `SyncPolicy::NoSync`. See
[Non-Durable Transactions](#non-durable-transactions).

**Reduce Lock Contention**

- Keep transactions short. Shorter transactions hold locks for less time, reducing
  the probability of blocking other threads.
- Minimize the lifetime of transactional cursors; read locks are held by default
  until the transaction ends.
- Access heavily-accessed records toward the end of the transaction to reduce the
  time that popular records are locked.
- Use committed-read isolation (`TransactionConfig::with_read_committed(true)`) for
  forward-scanning cursors. This releases read locks as the cursor advances rather
  than holding them for the entire transaction.
- Use uncommitted-read isolation where tolerable — this avoids taking read locks
  entirely for read operations.

**Isolation Level Trade-offs**

| Level | Locking Cost | Anomalies Possible |
|-------|--------------|--------------------|
| Read Uncommitted | Lowest (no read locks) | Dirty reads, non-repeatable reads, phantoms |
| Read Committed | Low (read locks released early) | Non-repeatable reads, phantoms |
| Repeatable Read (default) | Medium | Phantoms |
| Serializable | Highest (range locks) | None |

Choose the weakest isolation level that your application's correctness requirements
allow.

**Data Access Patterns**

If threads can be designed to operate on non-overlapping portions of the database,
lock contention is naturally minimized. Partition your data and assign disjoint
key ranges to different threads where possible.

**Deadlock Avoidance**

- Apply the same strategies as for reducing blocking.
- Ensure all threads lock records in the same order. Consistent lock ordering
  eliminates deadlocks (though not blocking).
- Use the read-modify-write pattern (`LockMode::Rmw`) when you know a read will be
  followed by a write, to avoid read-lock-to-write-lock upgrades that can deadlock.
- Expect deadlocks when using secondary databases in a concurrent application.
  Always implement the retry loop shown in [Aborting a Transaction](#aborting-a-transaction).

**Checkpointing**

Run checkpoints regularly to bound recovery time on restart. The checkpointer
daemon runs in the background by default (`EnvironmentConfig::with_run_checkpointer(true)`).
For high-write workloads you can tune the checkpoint interval to balance write
amplification against recovery time.

**Summary of Key Configuration Points**

| Goal | Configuration |
|------|--------------|
| Enable transactions | `EnvironmentConfig::with_transactional(true)` |
| Auto-commit a single write | Pass `None` for `txn` to `db.put()` / `db.delete()` |
| Relax durability for throughput | `Durability::COMMIT_WRITE_NO_SYNC` or `COMMIT_NO_SYNC` |
| Reduce read lock pressure | `TransactionConfig::with_read_committed(true)` |
| Allow dirty reads | `TransactionConfig::with_read_uncommitted(true)` |
| Prevent phantom reads | `TransactionConfig::with_serializable_isolation(true)` |
| Avoid lock-upgrade deadlocks | `LockMode::Rmw` on the initial read |
| Set environment lock timeout | `EnvironmentConfig::with_lock_timeout(ms)` |
| Set per-transaction lock timeout | `txn.set_lock_timeout(ms)` |
| Fail immediately on lock conflict | `TransactionConfig::with_no_wait(true)` |
| Bound recovery time | Keep checkpointer enabled (`with_run_checkpointer(true)`) |
