# Secondary Indices with Transactions

> **v1.5 limitations:** see
> [Getting Started → Secondary Databases → v1.5 limitations](../getting-started/secondary-databases.md#v15-limitations).
> v1.5 secondaries are one-to-one (Decision 1B); foreign-key constraints
> are rejected at `SecondaryDatabase::open` (Decision 2C). The chapter
> below describes the v1.6 contract; the parts that name APIs not yet
> wired in v1.5 will be rewritten in Sprint 4.

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

