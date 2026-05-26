# Cursors and Transactions

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
[Isolation Levels](isolation.md)).

---
