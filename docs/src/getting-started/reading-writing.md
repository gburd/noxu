# Reading and Writing

## Writing Records

Use `db.put` to insert or overwrite a record. Keys and values accept any
`impl AsRef<[u8]>` — byte literals, `&str`, `Vec<u8>`, `Bytes`, etc. — so no
`DatabaseEntry` wrapper is required:

```rust
db.put(b"user:alice", b"Alice Smith")?;
```

If a record with the same key already exists, `put` overwrites it by default.
The old value is lost. `put` returns `Result<()>`: success is `Ok(())`, and any
real failure (closed handle, read-only database, I/O error) is `Err(NoxuError)`.

`put` auto-commits (the write is immediately durable). To group multiple writes
into a single atomic unit, use `put_in` with an explicit transaction:

```rust
let txn = env.begin_transaction(None)?;
db.put_in(&txn, b"user:alice", b"Alice Smith")?;
db.put_in(&txn, b"user:bob", b"Bob Jones")?;
txn.commit()?;
```

## Reading Records

Use `db.get` to retrieve a record by key. It returns `Result<Option<Bytes>>` —
`Some(value)` if the key is present, `None` if absent:

```rust
match db.get(b"user:alice")? {
    Some(value) => {
        println!("Found: {}", std::str::from_utf8(&value)?);
    }
    None => {
        println!("No record for that key");
    }
}
```

To read inside a transaction use `get_in`:

```rust
let txn = env.begin_transaction(None)?;
if let Some(value) = db.get_in(&txn, b"user:alice")? {
    println!("{}", std::str::from_utf8(&value)?);
}
txn.commit()?;
```

For zero-allocation buffer reuse or partial reads, the lower-level
`get_into(txn, key, &mut DatabaseEntry)` escape hatch returns `Result<bool>`
(`true` if found) and reads into a caller-owned buffer.

## Deleting Records

Use `db.delete` to remove a record by key. It returns `Result<bool>` — `true`
if a record was removed, `false` if the key was absent:

```rust
if db.delete(b"user:alice")? {
    println!("Deleted");
} else {
    println!("Key did not exist");
}
```

Use `delete_in(&txn, key)` to delete inside a transaction.

## Result shapes

The idiomatic point operations collapse the historical
`Result<OperationStatus>` into Rust-native shapes (review P0-3):

| Operation | Returns | Meaning |
|---|---|---|
| `get` / `get_in` | `Result<Option<Bytes>>` | `Some(value)` = found, `None` = absent |
| `put` / `put_in` | `Result<()>` | `Ok(())` = written |
| `delete` / `delete_in` | `Result<bool>` | `true` = removed, `false` = absent |
| `put_no_overwrite` / `_in` | `Result<bool>` | `true` = inserted, `false` = key already existed |

Errors (I/O failures, lock timeouts, closed handles, etc.) are always returned
as `Err(NoxuError)`. The cursor API (`Cursor::get`/`put`/`delete`) still uses
`OperationStatus` for its lower-level navigation primitives.

## A Complete Read/Write Example

```rust
use noxu::{DatabaseConfig, Environment, EnvironmentConfig};
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
        db.put(key.as_bytes(), value.as_bytes())?;
    }

    // Read
    if let Some(value) = db.get(b"bob")? {
        println!("{}", std::str::from_utf8(&value)?);
    }

    // Delete
    db.delete(b"carol")?;

    println!("{} records remain", db.count()?);

    db.close()?;
    env.close()?;
    Ok(())
}
```

---
