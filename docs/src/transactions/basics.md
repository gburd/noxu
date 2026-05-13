# Introduction

This guide provides a thorough introduction to transactions as used with Noxu DB,
a Rust port of Noxu DB (Noxu DB 7.5.11). It covers the guarantees
that transactions provide, the application infrastructure required for full
transactional protection, and practical examples of writing transactional Rust code.

You should be familiar with the basic Noxu DB API — opening environments,
databases, and performing simple reads and writes — before reading this guide.

## Transaction Benefits (ACID)

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

## A Note on System Failure

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

## Recoverability

Durability means that once a transaction is committed, the database modifications
performed under its protection will not be lost due to system failure.

Noxu DB runs a normal recovery against a subset of its log files every time an
environment is opened. This is a routine procedure that ensures the database is in
a consistent state on startup. It is performed automatically and requires no
application intervention.

Noxu DB also supports archival backup and recovery in the case of catastrophic
failure such as the loss of a physical disk drive. See
[Backup and Recovery](#9-backup-and-recovery).

## Performance Tuning Overview

The use of transactions is not free. Transaction commits usually require disk I/O
that non-transactional applications do not perform. For multi-threaded applications,
transactions can increase lock contention due to extra locking required by
transactional isolation guarantees. Performance tuning considerations are discussed
throughout this guide and are summarized in [Performance Tuning](#10-performance-tuning).

---

# 2. Enabling Transactions

To use transactions you must:

1. Enable transactions on the environment using `EnvironmentConfig::with_transactional(true)`.
2. Open the environment before opening any databases.
3. Open databases using the same environment handle. Common practice is to use
   auto-commit for the database open (pass `None` for the transaction handle), so
   the open itself is automatically transaction-protected.

## Opening a Transactional Environment and Database

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

# 3. Transaction Basics

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

## Committing a Transaction

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

## Aborting a Transaction

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

## Non-Durable Transactions

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

## Auto-Commit

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

