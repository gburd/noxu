# Reading and Writing

## Writing Records

Use `db.put` to insert or overwrite a record:

```rust
use noxu_db::{DatabaseEntry, OperationStatus};

let key  = DatabaseEntry::from_bytes(b"user:alice");
let data = DatabaseEntry::from_bytes(b"Alice Smith");

let status = db.put(None, &key, &data)?;
assert_eq!(status, OperationStatus::Success);
```

If a record with the same key already exists, `put` overwrites it by default. The old value is lost.

The first argument to `put` is an optional `Transaction`. Pass `None` for non-transactional writes (the operation is immediately durable) or pass a transaction handle to group multiple writes into a single atomic unit.

## Reading Records

Use `db.get` to retrieve a record by key:

```rust
let key = DatabaseEntry::from_bytes(b"user:alice");
let mut data = DatabaseEntry::new();

match db.get(None, &key, &mut data)? {
    OperationStatus::Success => {
        println!("Found: {}", std::str::from_utf8(data.data())?);
    }
    OperationStatus::NotFound => {
        println!("No record for that key");
    }
    OperationStatus::KeyExists => {
        // returned by put-no-overwrite; not applicable to get
        unreachable!()
    }
}
```

`get` populates the `data` argument in place. On `NotFound`, `data` is left in an undefined state and should not be read.

## Deleting Records

Use `db.delete` to remove a record by key:

```rust
let key = DatabaseEntry::from_bytes(b"user:alice");
let status = db.delete(None, &key)?;

match status {
    OperationStatus::Success  => println!("Deleted"),
    OperationStatus::NotFound => println!("Key did not exist"),
    _ => {}
}
```

## OperationStatus

All read/write operations return `Result<OperationStatus>`. The `OperationStatus` enum has three variants:

| Variant | Meaning |
|---|---|
| `Success` | The operation completed successfully |
| `NotFound` | The key was not present in the database |
| `KeyExists` | A `put` with no-overwrite found that the key already exists |

Errors (I/O failures, lock timeouts, closed handles, etc.) are returned as `Err(NoxuError)`, not as `OperationStatus` values.

## A Complete Read/Write Example

```rust
use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus,
};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env = Environment::open(
        EnvironmentConfig::new(PathBuf::from("/tmp/myapp"))
            .with_allow_create(true)
    )?;

    let db = env.open_database(
        None,
        "contacts",
        &DatabaseConfig::new().with_allow_create(true),
    )?;

    // Write
    let contacts = [
        ("alice", "Alice Smith, Engineering"),
        ("bob",   "Bob Jones, Marketing"),
        ("carol", "Carol Wu, Engineering"),
    ];
    for (key, value) in &contacts {
        db.put(None, &DatabaseEntry::from_bytes(key.as_bytes()),
                     &DatabaseEntry::from_bytes(value.as_bytes()))?;
    }

    // Read
    let key = DatabaseEntry::from_bytes(b"bob");
    let mut data = DatabaseEntry::new();
    if db.get(None, &key, &mut data)? == OperationStatus::Success {
        println!("{}", std::str::from_utf8(data.data())?);
    }

    // Delete
    db.delete(None, &DatabaseEntry::from_bytes(b"carol"))?;

    println!("{} records remain", db.count()?);

    db.close()?;
    env.close()?;
    Ok(())
}
```

---

