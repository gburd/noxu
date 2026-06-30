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
| 3 | SERIALIZABLE | Repeatable read is observed: read locks are held for the full transaction duration (not released early as under read-committed). Phantom reads are prevented via **next-key range locking**: the cursor acquires `RangeRead` locks instead of plain `Read` locks, and new-key inserts acquire `RangeInsert` on the successor key's slot. A concurrent insert into the scanned range is blocked until the serializable transaction commits. |

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
    let _ = db.get_in(&txn, &key)?;

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

Serializable isolation prevents **phantom reads** via next-key range locking.

Under repeatable read (the default), a transaction T can perform a search that
returns `NotFound`, and then the same search can return `Success` later in the
same transaction if another transaction inserted a matching record in between.

With serializable isolation, Noxu DB prevents this by acquiring `RangeRead`
locks during cursor reads and `RangeInsert` locks during new-key insertions,
mirroring the next-key locking protocol from Berkeley DB JE:

1. **Cursor reads** acquire `LockType::RangeRead` (instead of `Read`) on each
   record's LSN.  A `RangeRead` lock conflicts with a concurrent `RangeInsert`
   on the same LSN, blocking the inserter or triggering a cursor restart.
2. **New-key inserts** acquire `LockType::RangeInsert` on the first committed
   key that sorts after the new key (the "next key" / successor).  If a
   serializable scanner holds `RangeRead` on that same successor slot, the
   insert is blocked until the scanner commits.
3. **End-of-range** protection: when a forward scan reaches the last key in
   the database, the cursor acquires `RangeRead` on a per-database EOF
   sentinel LSN.  A concurrent insert of a key that sorts after all
   currently-scanned keys acquires `RangeInsert` on the same sentinel and is
   blocked until the scanner commits.

This is proven by the tests:

- `test_serializable_prevents_phantom_insert` — insert into scanned range blocked
- `test_serializable_prevents_phantom_eof_insert` — append past EOF blocked
- `test_default_isolation_allows_phantom_insert` — regression: no over-locking
- `test_read_committed_allows_phantom_insert` — regression: RC unaffected

Serializable isolation causes additional locking (range locks) which can reduce
concurrency. Use it only when your application requires phantom prevention.

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
