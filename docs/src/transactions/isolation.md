# Isolation Levels

Isolation guarantees are a critical part of transactional protection. The stronger
the isolation, the more locking is required, which increases the chance of blocking
and reduces throughput. Relaxing isolation can improve performance but exposes your
application to anomalies.

## Supported Degrees of Isolation

| Degree | ANSI Term | Noxu Behavior |
|--------|-----------|---------------|
| 1 | READ UNCOMMITTED | Reads may see data modified but not yet committed by another transaction (dirty reads). A transaction may read data that is subsequently rolled back and never existed in the database. |
| 2 | READ COMMITTED | Dirty reads are prevented. Read locks are released as soon as the cursor moves past a record, rather than being held for the life of the transaction. Data at the current cursor position will not change, but previously read data can change after the cursor moves. |
| (default) | REPEATABLE READ | Read and write locks are held until the transaction completes. Data read by a transaction will not be modified by another transaction before the reading transaction completes. **This is the Noxu DB default.** |
| 3 | SERIALIZABLE | Repeatable read is observed plus no phantom reads. Phantoms are records that appear in a search result on a second execution that were absent on the first. Noxu DB prevents phantoms with additional range locking. |

By default, Noxu DB transactions use repeatable read isolation. You can configure
a lower level (uncommitted read, committed read) for performance or a higher level
(serializable) for correctness when phantoms are a concern.

## Reading Uncommitted Data

Uncommitted reads (dirty reads) allow one transaction to see data that has been
modified but not yet committed by another. This can improve performance by
eliminating read locks, but the data you read may subsequently be rolled back.

Configure uncommitted reads at the transaction level:

```rust
use noxu::{
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
    let txn = env.begin_transaction(Some(&txn_config))?;

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
use noxu::{DatabaseEntry, LockMode};

// (env, db, txn assumed to be open)
let key = DatabaseEntry::from_bytes(b"thekey");
let mut data = DatabaseEntry::new();

// Pass the lock mode directly to the get call.
db.get_with_lock_mode(Some(&txn), &key, &mut data, LockMode::ReadUncommitted)?;
```

## Committed Reads

Read committed isolation means read locks are released as soon as the cursor
advances past a record, rather than being held for the transaction's lifetime. This
allows other transactions to modify records that have already been read and moved
past, potentially improving throughput when you are scanning forward through data
and do not need to re-read previous records.

```rust
use noxu::TransactionConfig;

// Use read committed isolation for this transaction.
let txn_config = TransactionConfig::new().with_read_committed(true);
let txn = env.begin_transaction(Some(&txn_config))?;
```

Or per-operation:

```rust
use noxu::LockMode;

db.get_with_lock_mode(Some(&txn), &key, &mut data, LockMode::ReadCommitted)?;
```

Read committed is most useful for forward-scanning cursors that never need to
re-read previously visited records.

## Serializable Isolation

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
use noxu::{Environment, EnvironmentConfig};
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
use noxu::TransactionConfig;

// Serializable isolation is achieved by combining serializable flag
// with the transaction config.
let txn_config = TransactionConfig::new()
    .with_serializable_isolation(true);
let txn = env.begin_transaction(Some(&txn_config))?;
```

---
