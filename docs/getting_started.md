# Getting Started with Noxu DB

Noxu DB is a general-purpose, transaction-protected, embedded key-value database written in pure Rust. It is a faithful port of Berkeley DB Java Edition (BDB JE 7.5.11) to Rust, retaining the same storage architecture, log-structured B-tree, and ACID transaction semantics while providing idiomatic Rust APIs.

This guide covers everything a Rust developer needs to start using Noxu DB: opening environments and databases, reading and writing records, iterating with cursors, working with typed bindings, managing secondary indexes, and administering environments for production use.

---

## Table of Contents

1. [Introduction and Overview](#1-introduction-and-overview)
2. [Database Environments](#2-database-environments)
3. [Databases](#3-databases)
4. [Database Records](#4-database-records)
5. [Reading and Writing](#5-reading-and-writing)
6. [Cursors](#6-cursors)
7. [Secondary Databases](#7-secondary-databases)
8. [The Binding Layer](#8-the-binding-layer)
9. [Transactions](#9-transactions)
10. [Error Handling](#10-error-handling)
11. [Environment Administration](#11-environment-administration)
12. [Backup and Recovery](#12-backup-and-recovery)

---

## 1. Introduction and Overview

### What is Noxu DB?

Noxu DB is an embedded, transactional key-value store. "Embedded" means it runs inside your application process — there is no separate server to start or manage. "Transactional" means it provides full ACID guarantees: Atomicity, Consistency, Isolation, and Durability.

Key characteristics:

- All data is stored as raw byte arrays (`&[u8]`). Any Rust type that can be serialized to bytes can be stored.
- Records consist of a key/data pair. Keys are used to look up data. Both keys and data are represented by `DatabaseEntry` objects.
- The B-tree is always sorted by key, so range scans are efficient.
- One or more databases live inside a single *environment*. The environment manages the shared cache, background threads, and the on-disk log files.
- Transactions are optional but recommended for any application that writes data.

### Architecture in Brief

A Noxu DB application has three layers:

```
Environment
  └── Database (named, multiple per environment)
        └── Records (key/data pairs in a B-tree)
```

All data is stored in sequentially numbered log files (`.ndb` extension) in the environment directory. There is no separate "database file" distinct from the log — the log is the database. When the environment is opened, Noxu DB performs normal recovery to bring the B-tree back to a consistent state from the log.

### Adding Noxu DB to a Project

Add the following to your `Cargo.toml`:

```toml
[dependencies]
noxu-db = { path = "crates/noxu-db" }

# Optional: typed bindings for integers, floats, strings
noxu-bind = { path = "crates/noxu-bind" }
```

---

## 2. Database Environments

### What is an Environment?

An environment is a directory on disk plus an in-memory handle that manages everything in that directory. Every application using Noxu DB must use an environment — it is not optional. The environment:

- Provides the in-memory cache shared by all databases opened through it.
- Runs background threads (cleaner, checkpointer, evictor).
- Manages lock and transaction state.
- Corresponds to a specific directory path on disk.

### Opening an Environment

Use `Environment::open` with an `EnvironmentConfig`:

```rust
use noxu_db::{Environment, EnvironmentConfig};
use std::path::PathBuf;

let config = EnvironmentConfig::new(PathBuf::from("/var/data/myapp"))
    .with_allow_create(true)   // create the directory if it does not exist
    .with_transactional(true); // enable transactional support

let env = Environment::open(config)?;
```

If `with_allow_create(false)` (the default) and the directory does not exist, `open` returns an error. The directory must exist, or `allow_create` must be `true`.

### Environment Configuration

`EnvironmentConfig` uses a builder pattern. All configuration is set before opening; it cannot be changed while the environment is open.

```rust
use noxu_db::{Environment, EnvironmentConfig};

let config = EnvironmentConfig::new(PathBuf::from("/var/data/myapp"))
    .with_allow_create(true)
    .with_transactional(true)
    .with_cache_size(256 * 1024 * 1024)  // 256 MB cache
    .with_read_only(false);

let env = Environment::open(config)?;
```

Key configuration fields:

| Field | Default | Description |
|---|---|---|
| `allow_create` | `false` | Create the environment directory if it does not exist |
| `transactional` | `false` | Enable transaction support |
| `read_only` | `false` | Open the environment in read-only mode |
| `cache_size` | 64 MB | Maximum in-memory cache size in bytes |
| `lock_timeout_ms` | 500 | Milliseconds before a lock attempt times out |
| `txn_timeout_ms` | 0 | Transaction timeout in milliseconds (0 = none) |
| `run_cleaner` | `true` | Run the log cleaner background thread |
| `run_checkpointer` | `true` | Run the checkpointer background thread |
| `run_evictor` | `true` | Run the cache evictor background thread |

You can also use the mutable setter form if you need to configure fields that do not have builder-style methods:

```rust
let mut config = EnvironmentConfig::new(PathBuf::from("/data"));
config.set_allow_create(true);
config.set_cache_size(128 * 1024 * 1024);
config.set_run_cleaner(false); // disable cleaner for bulk load
let env = Environment::open(config)?;
```

### Read-Only Environments

A read-only environment can be opened against an environment directory that is currently being written by another process. No write operations are permitted. Background threads do not run in a read-only environment.

```rust
let config = EnvironmentConfig::new(PathBuf::from("/var/data/myapp"))
    .with_read_only(true);
let env = Environment::open(config)?;
assert!(env.is_read_only());
```

### Closing an Environment

Always close the environment when you are finished. All open database handles must be closed first, and there must be no active transactions.

```rust
// Close databases first
db.close()?;

// Then close the environment
env.close()?;
```

If the environment handle goes out of scope without being explicitly closed, the `Drop` implementation performs a best-effort close. Relying on `Drop` is acceptable for simple applications but explicit close is recommended to propagate any errors.

### Listing Databases in an Environment

```rust
let names: Vec<String> = env.get_database_names()?;
for name in &names {
    println!("database: {}", name);
}
```

### Renaming and Removing Databases

```rust
// Rename (the database must not currently be open)
env.rename_database(None, "old_name", "new_name")?;

// Remove permanently
env.remove_database(None, "db_name")?;
```

---

## 3. Databases

### What is a Database?

A Noxu DB database is a named B-tree stored within an environment. Each database holds a collection of key/data records. You can think of it as a sorted map from byte-array keys to byte-array values.

Multiple databases can coexist in the same environment. They share the environment's cache and background threads but are otherwise independent B-trees.

On disk, all databases in an environment are stored together in the environment's log files — there are no separate per-database files.

### Opening a Database

Databases are opened through the environment handle:

```rust
use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};

let env = Environment::open(
    EnvironmentConfig::new(PathBuf::from("/data"))
        .with_allow_create(true)
)?;

let db_config = DatabaseConfig::new().with_allow_create(true);
let db = env.open_database(None, "my_database", &db_config)?;
```

The first argument to `open_database` is an optional transaction handle. When `None` is passed the open is non-transactional (the most common case for database opens).

The second argument is the database name. Names are arbitrary strings. An empty name is an error.

By default Noxu DB will not create a database that does not exist. You must set `with_allow_create(true)` on the `DatabaseConfig` for the first open.

### Database Configuration

```rust
use noxu_db::DatabaseConfig;

let config = DatabaseConfig::new()
    .with_allow_create(true)    // create if it does not exist
    .with_read_only(false)      // allow writes
    .with_transactional(true)   // participate in transactions
    .with_sorted_duplicates(false); // do not allow duplicate keys (default)
```

Key configuration fields:

| Field | Default | Description |
|---|---|---|
| `allow_create` | `false` | Create the database if it does not already exist |
| `read_only` | `false` | Open the database in read-only mode |
| `transactional` | `false` | Allow the database to participate in transactions |
| `sorted_duplicates` | `false` | Allow multiple records with the same key |
| `temporary` | `false` | In-memory only; deleted when closed |

### Multiple Databases in One Environment

```rust
let customers = env.open_database(None, "customers", &DatabaseConfig::new().with_allow_create(true))?;
let orders    = env.open_database(None, "orders",    &DatabaseConfig::new().with_allow_create(true))?;
let products  = env.open_database(None, "products",  &DatabaseConfig::new().with_allow_create(true))?;
```

All three databases share the environment's cache and can participate in the same transactions.

### Closing a Database

```rust
db.close()?;
```

After calling `close`, the handle can no longer be used. Any active cursors on the database are invalidated. Always close all cursors before closing the database.

### Checking Whether a Database Handle is Valid

```rust
if db.is_valid() {
    // safe to use
}
```

### Getting the Record Count

```rust
let count: u64 = db.count()?;
println!("{} records in database", count);
```

---

## 4. Database Records

### The DatabaseEntry Type

Every Noxu DB record consists of two parts: a key and a data value. Both are represented as `DatabaseEntry` objects, which are essentially wrappers around a byte slice (`&[u8]`).

`DatabaseEntry` is the universal container for moving data in and out of the database. Any type that can be serialized to bytes can be stored in Noxu DB.

### Creating DatabaseEntry Objects

```rust
use noxu_db::DatabaseEntry;

// From a byte literal
let key = DatabaseEntry::from_bytes(b"employee:1001");

// From a String (always use explicit UTF-8 encoding)
let name = "Alice".to_string();
let key = DatabaseEntry::from_bytes(name.as_bytes());

// From a Vec<u8>
let raw: Vec<u8> = vec![0x01, 0x02, 0x03];
let entry = DatabaseEntry::from_vec(raw);

// Empty entry (used as an output buffer for get operations)
let mut data_out = DatabaseEntry::new();
```

### Reading Data Back

After a `get` operation populates a `DatabaseEntry`, use `.data()` to access the raw bytes:

```rust
let mut data = DatabaseEntry::new();
let status = db.get(None, &key, &mut data)?;
if status == OperationStatus::Success {
    let bytes: &[u8] = data.data();
    let text = std::str::from_utf8(bytes)?;
    println!("Got: {}", text);
}
```

Use `.get_data()` when you want an `Option<&[u8]>` (returns `None` for an empty entry):

```rust
if let Some(bytes) = data.get_data() {
    // bytes is &[u8]
}
```

### Encoding Structured Data

Because `DatabaseEntry` stores raw bytes, you must decide how to encode your application's data types. Options include:

- **UTF-8 strings** — human-readable, easy for debugging.
- **`bincode` or `serde` serialization** — compact, works with any `Serialize`/`Deserialize` type.
- **Noxu bind APIs** — sort-preserving encodings for integers, floats, and strings (described in [Section 8](#8-the-binding-layer)).
- **Custom encoding** — write fields in a fixed order for maximum control over sort order.

Example using `bincode` for a structured value:

```rust
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Employee {
    id: u64,
    name: String,
    department: String,
    salary: f64,
}

// Serialize to bytes
let employee = Employee { id: 1001, name: "Alice".into(), department: "Engineering".into(), salary: 95000.0 };
let encoded = bincode::serialize(&employee)?;
let data_entry = DatabaseEntry::from_vec(encoded);

// Deserialize from bytes
let bytes = data_entry.data();
let decoded: Employee = bincode::deserialize(bytes)?;
```

### Key Design

Key design has a direct impact on performance and sort order. Because records are sorted lexicographically by key bytes:

- Numeric keys encoded as big-endian integers sort correctly as unsigned values. The Noxu bind APIs provide sort-preserving encodings for signed integers and floating-point numbers.
- String keys in UTF-8 sort in lexicographic order, which is usually correct for text data.
- Composite keys (e.g., `namespace:id`) enable prefix scans: iterate all records in a namespace by seeking to `namespace:` and reading forward.

---

## 5. Reading and Writing

### Writing Records

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

### Reading Records

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

### Deleting Records

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

### OperationStatus

All read/write operations return `Result<OperationStatus>`. The `OperationStatus` enum has three variants:

| Variant | Meaning |
|---|---|
| `Success` | The operation completed successfully |
| `NotFound` | The key was not present in the database |
| `KeyExists` | A `put` with no-overwrite found that the key already exists |

Errors (I/O failures, lock timeouts, closed handles, etc.) are returned as `Err(NoxuError)`, not as `OperationStatus` values.

### A Complete Read/Write Example

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

## 6. Cursors

### What is a Cursor?

A cursor is a position marker that can move through a database's records in sorted key order. Cursors allow you to:

- Iterate forward or backward through all records.
- Seek to a specific key or to the nearest key that is greater-than-or-equal to a target.
- Insert, update, or delete records at the current cursor position.

Cursors are the primary tool for bulk reads, range scans, and operating on databases that allow duplicate keys.

### Opening and Closing Cursors

```rust
let mut cursor = db.open_cursor(None, None)?;

// ... use cursor ...

cursor.close()?;
```

The first argument is an optional transaction. The second is an optional `CursorConfig`. Both are typically `None` for simple use cases.

Cursors must be closed before the database they belong to is closed. Failing to close cursors before closing a database returns an error.

### Navigating with Get

All cursor navigation is done through a single method with a `Get` enum that specifies the movement:

```rust
use noxu_db::Get;

let mut key  = DatabaseEntry::new();
let mut data = DatabaseEntry::new();

let status = cursor.get(&mut key, &mut data, Get::First, None)?;
```

The `Get` variants:

| Variant | Behavior |
|---|---|
| `Get::First` | Move to the first record (smallest key) |
| `Get::Last` | Move to the last record (largest key) |
| `Get::Next` | Move to the next record |
| `Get::Prev` | Move to the previous record |
| `Get::Search` | Move to the record with exactly the given key |
| `Get::SearchGte` | Move to the first record with key >= the given key |
| `Get::SearchRange` | Alias for `SearchGte` (matches `getSearchKeyRange` in JE) |
| `Get::Current` | Re-read the record at the current position |

For `Search`, `SearchGte`, and `SearchRange`, the key to search for must be placed in the key `DatabaseEntry` before calling `get`. After a successful `Search` the key entry holds the found key; after `SearchGte` the key entry holds the actual key found (which may be greater than the search key).

### Forward Iteration

```rust
let mut cursor = db.open_cursor(None, None)?;
let mut key  = DatabaseEntry::new();
let mut data = DatabaseEntry::new();

let mut status = cursor.get(&mut key, &mut data, Get::First, None)?;
while status == OperationStatus::Success {
    println!(
        "{} = {}",
        std::str::from_utf8(key.data())?,
        std::str::from_utf8(data.data())?
    );
    status = cursor.get(&mut key, &mut data, Get::Next, None)?;
}
cursor.close()?;
```

### Reverse Iteration

```rust
let mut cursor = db.open_cursor(None, None)?;
let mut key  = DatabaseEntry::new();
let mut data = DatabaseEntry::new();

let mut status = cursor.get(&mut key, &mut data, Get::Last, None)?;
while status == OperationStatus::Success {
    println!("{} = {}", std::str::from_utf8(key.data())?, std::str::from_utf8(data.data())?);
    status = cursor.get(&mut key, &mut data, Get::Prev, None)?;
}
cursor.close()?;
```

### Searching for a Specific Key

```rust
let mut cursor = db.open_cursor(None, None)?;
let mut search_key = DatabaseEntry::from_bytes(b"carol");
let mut data = DatabaseEntry::new();

let status = cursor.get(&mut search_key, &mut data, Get::Search, None)?;
if status == OperationStatus::Success {
    println!("Found: {}", std::str::from_utf8(data.data())?);
} else {
    println!("Not found");
}
cursor.close()?;
```

### Range Scan (Greater-Than-Or-Equal Search)

`Get::SearchGte` (or its alias `Get::SearchRange`) positions the cursor at the first record with a key that is greater than or equal to the search key. This is the key primitive for prefix and range scans:

```rust
let mut cursor = db.open_cursor(None, None)?;
let mut range_key = DatabaseEntry::from_bytes(b"user:m");  // start of range
let mut data = DatabaseEntry::new();

let mut status = cursor.get(&mut range_key, &mut data, Get::SearchGte, None)?;
while status == OperationStatus::Success {
    let k = std::str::from_utf8(range_key.data())?;
    if !k.starts_with("user:") {
        break; // left the user: namespace
    }
    println!("{} = {}", k, std::str::from_utf8(data.data())?);
    status = cursor.get(&mut range_key, &mut data, Get::Next, None)?;
}
cursor.close()?;
```

### Deleting via Cursor

`cursor.delete()` removes the record at the current cursor position. The cursor must have been successfully positioned (i.e., the most recent `get` returned `Success`) before calling `delete`.

```rust
let mut cursor = db.open_cursor(None, None)?;
let mut search_key = DatabaseEntry::from_bytes(b"user:bob");
let mut data = DatabaseEntry::new();

if cursor.get(&mut search_key, &mut data, Get::Search, None)? == OperationStatus::Success {
    cursor.delete()?;
}
cursor.close()?;
```

### Writing via Cursor

`cursor.put` inserts or overwrites the record at the current cursor position. Use the `Put` enum to control overwrite behavior:

```rust
use noxu_db::Put;

let key  = DatabaseEntry::from_bytes(b"user:dave");
let data = DatabaseEntry::from_bytes(b"Dave Brown, Finance");
cursor.put(&key, &data, Put::Overwrite)?;
```

`Put::Overwrite` replaces any existing record with the given key. `Put::NoOverwrite` returns `OperationStatus::KeyExists` if the key already exists.

### Replacing Data via Cursor

To update the data for the current cursor position without changing the key:

```rust
// Position cursor on the record to update
let mut search_key = DatabaseEntry::from_bytes(b"user:alice");
let mut old_data = DatabaseEntry::new();
cursor.get(&mut search_key, &mut old_data, Get::Search, None)?;

// Replace the data
let new_data = DatabaseEntry::from_bytes(b"Alice Smith, VP Engineering");
cursor.put(&search_key, &new_data, Put::Overwrite)?;
```

### Important: Always Close Cursors

Cursors hold page locks. Open cursors consume resources and can block other threads. Always close cursors as soon as you are done with them — preferably in a `defer`-style pattern or at the end of a lexical scope using Rust's RAII.

---

## 7. Secondary Databases

### What is a Secondary Database?

A secondary database is an index over a primary database. While the primary database stores your canonical records keyed by a primary key (e.g., employee ID), a secondary database stores an additional mapping from some derived key (e.g., department name) to the primary key.

Secondary databases are read-only from your application's perspective — you do not insert into them directly. Instead, whenever you update the primary database, you update the secondary index to reflect the change.

### Implementing a Key Creator

A key creator extracts the secondary key from a primary record. Implement the `SecondaryKeyCreator` trait:

```rust
use noxu_db::{Database, DatabaseEntry, SecondaryKeyCreator};

struct DepartmentKeyCreator;

impl SecondaryKeyCreator for DepartmentKeyCreator {
    fn create_secondary_key(
        &self,
        _secondary_db: &Database,
        _primary_key: &DatabaseEntry,
        primary_data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        // Data format: "department|title"
        if let Some(bytes) = primary_data.get_data() {
            if let Ok(s) = std::str::from_utf8(bytes) {
                if let Some(sep) = s.find('|') {
                    result.set_data(s[..sep].as_bytes());
                    return true;
                }
            }
        }
        false // return false to indicate no secondary key for this record
    }
}
```

The method returns `true` if a secondary key was produced, or `false` if this primary record should have no entry in the secondary database.

### Opening a Secondary Database

```rust
use noxu_db::{SecondaryConfig, SecondaryDatabase};
use std::sync::Arc;
use parking_lot::Mutex;

// Open primary database
let primary_db = env.open_database(None, "employees",
    &DatabaseConfig::new().with_allow_create(true))?;
let primary = Arc::new(Mutex::new(primary_db));

// Open the underlying storage database for the secondary index
let sec_db = env.open_database(None, "by_department",
    &DatabaseConfig::new().with_allow_create(true))?;

// Create and open the secondary database
let sec_config = SecondaryConfig::new()
    .with_allow_create(true)
    .with_allow_populate(true)
    .with_key_creator(Box::new(DepartmentKeyCreator));

let secondary = SecondaryDatabase::open(Arc::clone(&primary), sec_db, sec_config)?;
```

### Reading from a Secondary Database

```rust
let dept_key = DatabaseEntry::from_bytes(b"Engineering");
let mut primary_key = DatabaseEntry::new();
let mut data = DatabaseEntry::new();

let status = secondary.get(None, &dept_key, &mut primary_key, &mut data)?;
if status == OperationStatus::Success {
    let emp_name = std::str::from_utf8(primary_key.data())?;
    let record   = std::str::from_utf8(data.data())?;
    println!("{}: {}", emp_name, record);
}
```

A secondary `get` returns three values: the secondary key, the primary key, and the primary data.

### Iterating a Secondary Database

```rust
let mut cursor = secondary.open_cursor(None, None)?;
let mut sec_key = DatabaseEntry::new();
let mut pk      = DatabaseEntry::new();
let mut data    = DatabaseEntry::new();

let mut status = cursor.get_first(&mut sec_key, &mut pk, &mut data)?;
while status == OperationStatus::Success {
    let dept = std::str::from_utf8(sec_key.data())?;
    let name = std::str::from_utf8(pk.data())?;
    println!("{}: {}", dept, name);
    status = cursor.get_next(&mut sec_key, &mut pk, &mut data)?;
}
cursor.close()?;
```

### Keeping the Secondary Index in Sync

When you insert or update a primary record, call `secondary.update_secondary` to keep the index consistent:

```rust
// Insert into primary
let key   = DatabaseEntry::from_bytes(b"Alice");
let value = DatabaseEntry::from_bytes(b"Engineering|Senior Engineer");
primary.lock().put(None, &key, &value)?;

// Update secondary index
secondary.update_secondary(&key, None, Some(&value))?;
// Arguments: primary_key, old_data (None for insert), new_data (None for delete)
```

For updates, provide both old and new data:

```rust
let old_value = DatabaseEntry::from_bytes(b"Engineering|Senior Engineer");
let new_value = DatabaseEntry::from_bytes(b"Engineering|Staff Engineer");
primary.lock().put(None, &key, &new_value)?;
secondary.update_secondary(&key, Some(&old_value), Some(&new_value))?;
```

For deletes, provide only the old data:

```rust
secondary.update_secondary(&key, Some(&old_value), None)?;
primary.lock().delete(None, &key)?;
```

### Closing a Secondary Database

```rust
secondary.close()?;
```

The secondary must be closed before the primary database and before the environment.

---

## 8. The Binding Layer

### Why Bindings?

`DatabaseEntry` holds raw bytes. To store typed Rust values with sort-preserving key encodings, Noxu DB provides the `noxu-bind` crate. The binding layer converts typed values to and from byte arrays in a way that:

- Preserves sort order: sorted byte comparison produces the same order as sorted value comparison.
- Is compact and fast to encode/decode.
- Handles edge cases like negative integers and NaN-free floating-point values.

### Available Bindings

Add `noxu-bind` to your `Cargo.toml`:

```toml
[dependencies]
noxu-bind = { path = "crates/noxu-bind" }
```

Available bindings in `noxu_bind`:

| Type | Binding | Notes |
|---|---|---|
| `i32` | `IntBinding` | Sort-preserving signed 32-bit integer |
| `i64` | `LongBinding` | Sort-preserving signed 64-bit integer |
| `f64` | `SortedDoubleBinding` | Sort-preserving IEEE 754 double |
| `String` | `StringBinding` | UTF-8 string, null-byte safe |

All bindings implement the `EntryBinding<T>` trait with two methods:
- `object_to_entry(&self, value: &T, entry: &mut DatabaseEntry)` — encode value into entry
- `entry_to_object(&self, entry: &DatabaseEntry) -> Result<T>` — decode entry back to value

### Integer Keys

```rust
use noxu_bind::{EntryBinding, IntBinding};
use noxu_db::{DatabaseEntry, OperationStatus};

let binding = IntBinding::new();

// Store an integer key
let mut key_entry = DatabaseEntry::new();
let value: i32 = 42;
binding.object_to_entry(&value, &mut key_entry)?;
db.put(None, &key_entry, &DatabaseEntry::from_bytes(b"forty-two"))?;

// Look up by integer key
let mut search_key = DatabaseEntry::new();
binding.object_to_entry(&42i32, &mut search_key)?;
let mut data = DatabaseEntry::new();
if db.get(None, &search_key, &mut data)? == OperationStatus::Success {
    println!("{}", std::str::from_utf8(data.data())?);
}
```

Because `IntBinding` produces sort-preserving byte encodings, records are stored and retrieved in numeric order. `i32::MIN` sorts before -1 sorts before 0 sorts before 1 sorts before `i32::MAX`.

### String Keys

```rust
use noxu_bind::{EntryBinding, StringBinding};

let binding = StringBinding::new();

let mut key_entry = DatabaseEntry::new();
binding.object_to_entry(&"Alice".to_string(), &mut key_entry)?;
db.put(None, &key_entry, &DatabaseEntry::from_bytes(b"alice's data"))?;

// Decode a string from an entry after retrieval
let recovered: String = binding.entry_to_object(&key_entry)?;
assert_eq!(recovered, "Alice");
```

### Sorted Double Keys

```rust
use noxu_bind::{EntryBinding, SortedDoubleBinding};

let binding = SortedDoubleBinding::new();

let temperatures = [-273.15f64, -40.0, 0.0, 37.0, 100.0];
for &temp in &temperatures {
    let mut key_entry = DatabaseEntry::new();
    binding.object_to_entry(&temp, &mut key_entry)?;
    let label = format!("{:.2}°C", temp);
    db.put(None, &key_entry, &DatabaseEntry::from_bytes(label.as_bytes()))?;
}
// When iterated, records appear in ascending numeric temperature order.
```

### Long Keys with Round-Trip

```rust
use noxu_bind::{EntryBinding, LongBinding};

let binding = LongBinding::new();

let mut key_entry = DatabaseEntry::new();
binding.object_to_entry(&i64::MAX, &mut key_entry)?;

// ... store and retrieve ...

let mut data_entry = DatabaseEntry::new();
db.get(None, &key_entry, &mut data_entry)?;
let recovered: i64 = binding.entry_to_object(&data_entry)?;
```

### Custom Encodings

For complex types you implement your own encoding. Write the fields to a `Vec<u8>` in the order that determines sort priority. The first bytes written have the highest sort weight.

```rust
struct Point { x: i32, y: i32 }

fn encode_point(p: &Point) -> DatabaseEntry {
    let mut buf = Vec::with_capacity(8);
    // Sort by x first, then y (big-endian so bytes sort correctly)
    buf.extend_from_slice(&(p.x ^ i32::MIN).to_be_bytes()); // sign-bit flip for signed sort
    buf.extend_from_slice(&(p.y ^ i32::MIN).to_be_bytes());
    DatabaseEntry::from_vec(buf)
}

fn decode_point(entry: &DatabaseEntry) -> Point {
    let bytes = entry.data();
    let x = i32::from_be_bytes(bytes[0..4].try_into().unwrap()) ^ i32::MIN;
    let y = i32::from_be_bytes(bytes[4..8].try_into().unwrap()) ^ i32::MIN;
    Point { x, y }
}
```

This technique (XOR with `MIN` before big-endian encoding) is the same approach used internally by `IntBinding` and `LongBinding`.

---

## 9. Transactions

### What Transactions Provide

Transactions give your operations three guarantees:

- **Atomicity**: a group of operations either all succeed or all are rolled back. The database is never left in a partial state.
- **Isolation**: a transaction sees a consistent view of the database that is not disturbed by concurrent writers. Changes made within a transaction are not visible to other transactions until committed.
- **Durability**: once a transaction commits, its changes survive crashes and process restarts.

### Enabling Transactional Support

Both the environment and the database must be configured for transactions:

```rust
let env = Environment::open(
    EnvironmentConfig::new(PathBuf::from("/data"))
        .with_allow_create(true)
        .with_transactional(true),   // required
)?;

let db = env.open_database(
    None,
    "accounts",
    &DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true),   // required
)?;
```

### Beginning, Committing, and Aborting Transactions

```rust
// Begin a transaction
let txn = env.begin_transaction(None, None)?;

// Use it in database operations
let key  = DatabaseEntry::from_bytes(b"account:alice");
let data = DatabaseEntry::from_bytes(b"balance:10000");
db.put(Some(&txn), &key, &data)?;

// Commit to make changes permanent
txn.commit()?;
```

To roll back all changes in a transaction:

```rust
let txn = env.begin_transaction(None, None)?;

db.put(Some(&txn), &key1, &data1)?;
db.put(Some(&txn), &key2, &data2)?;

// Something went wrong — roll back everything
txn.abort()?;
```

After `commit` or `abort`, the transaction handle is invalid. Calling `commit` or `abort` a second time returns an error.

### Atomic Multi-Record Update

A common pattern: transfer a balance from one account to another atomically.

```rust
let txn = env.begin_transaction(None, None)?;

// Debit Alice
let mut alice_data = DatabaseEntry::new();
db.get(Some(&txn), &DatabaseEntry::from_bytes(b"account:alice"), &mut alice_data)?;
let new_alice = update_balance(alice_data.data(), -500);
db.put(Some(&txn), &DatabaseEntry::from_bytes(b"account:alice"),
       &DatabaseEntry::from_vec(new_alice))?;

// Credit Bob
let mut bob_data = DatabaseEntry::new();
db.get(Some(&txn), &DatabaseEntry::from_bytes(b"account:bob"), &mut bob_data)?;
let new_bob = update_balance(bob_data.data(), 500);
db.put(Some(&txn), &DatabaseEntry::from_bytes(b"account:bob"),
       &DatabaseEntry::from_vec(new_bob))?;

// Both changes committed atomically
txn.commit()?;
```

If the process crashes between the debit and credit, Noxu DB's normal recovery on next open will roll back the partial transaction.

### Checking Transaction State

```rust
let txn = env.begin_transaction(None, None)?;
assert!(txn.is_valid());
println!("transaction id: {}", txn.get_id());

txn.commit()?;
assert!(!txn.is_valid()); // no longer valid after commit
```

### Lock Conflicts and Deadlocks

When multiple transactions attempt to lock the same record, one may be forced to wait. If both transactions are waiting for each other, a deadlock is detected and one is chosen as the victim and receives a `NoxuError::DeadlockDetected` error.

The standard pattern for handling deadlocks is to abort and retry:

```rust
loop {
    let txn = env.begin_transaction(None, None)?;
    match do_work(&db, &txn) {
        Ok(()) => {
            txn.commit()?;
            break;
        }
        Err(NoxuError::DeadlockDetected) | Err(NoxuError::LockConflict(_)) => {
            txn.abort()?;
            // retry
        }
        Err(e) => {
            txn.abort()?;
            return Err(e);
        }
    }
}
```

---

## 10. Error Handling

### The NoxuError Enum

All fallible Noxu DB operations return `Result<T, NoxuError>`. The `NoxuError` enum covers all error conditions:

```rust
use noxu_db::NoxuError;

match result {
    Ok(status) => { /* success */ }
    Err(NoxuError::DatabaseNotFound(name)) => {
        eprintln!("database '{}' does not exist", name);
    }
    Err(NoxuError::EnvironmentClosed) => {
        eprintln!("attempted operation on a closed environment");
    }
    Err(NoxuError::DeadlockDetected) => {
        // retry the transaction
    }
    Err(NoxuError::LockConflict(msg)) => {
        eprintln!("lock conflict: {}", msg);
    }
    Err(NoxuError::Timeout) => {
        eprintln!("operation timed out");
    }
    Err(e) => {
        eprintln!("unexpected error: {}", e);
    }
}
```

### Full Error Variant Reference

| Variant | Meaning |
|---|---|
| `EnvironmentFailure(String)` | Fatal condition; the environment must be closed |
| `DatabaseNotFound(String)` | Named database does not exist and `allow_create` is `false` |
| `DatabaseAlreadyExists(String)` | Attempted to create a database that already exists |
| `LockConflict(String)` | A lock could not be acquired (timeout or contention) |
| `DeadlockDetected` | Deadlock between two or more transactions |
| `TransactionAborted(String)` | The transaction was rolled back |
| `CursorClosed` | Operation on a closed cursor |
| `IllegalArgument(String)` | Invalid argument (e.g., empty database name) |
| `OperationNotAllowed(String)` | Operation not permitted in the current state |
| `DatabaseClosed` | Operation on a closed database handle |
| `EnvironmentClosed` | Operation on a closed environment handle |
| `IoError(std::io::Error)` | Underlying I/O error |
| `NotFound` | Key not found (rare; most not-found cases use `OperationStatus::NotFound`) |
| `KeyExists` | Key already exists (for no-overwrite operations) |
| `SecondaryIntegrityException(String)` | Secondary index inconsistency |
| `ReadOnly` | Write operation attempted on a read-only database or environment |
| `Timeout` | Operation timed out |
| `InvalidOperation(String)` | Operation is not valid given the current state |

### Error Propagation

Use the `?` operator to propagate errors naturally through your call stack:

```rust
fn load_config(db: &Database, key: &str) -> Result<String, NoxuError> {
    let key_entry  = DatabaseEntry::from_bytes(key.as_bytes());
    let mut data   = DatabaseEntry::new();
    let status = db.get(None, &key_entry, &mut data)?;
    if status == OperationStatus::NotFound {
        return Err(NoxuError::NotFound);
    }
    Ok(std::str::from_utf8(data.data())
        .map_err(|e| NoxuError::IllegalArgument(e.to_string()))?
        .to_string())
}
```

---

## 11. Environment Administration

### Background Threads

Noxu DB runs several background threads inside the environment process. They are started once per environment open and run until the environment is closed. All three are enabled by default.

**Cleaner thread** (`run_cleaner`): Scans log files and reclaims space occupied by deleted or overwritten records. The cleaner ensures that log files do not grow without bound. It only runs when the environment is open for write access. The cleaner's minimum utilization target is configurable (default: 50% — at least half the space in any log file must be live records before the cleaner considers the file fully utilized).

**Checkpointer thread** (`run_checkpointer`): Periodically flushes dirty cache pages to the log and writes a checkpoint record. Checkpoints shorten recovery time after a crash: recovery only needs to replay log entries since the last checkpoint. The checkpointer always runs (even in non-transactional environments).

**Evictor thread** (`run_evictor`): Evicts cold cache pages to keep cache usage within the configured `cache_size` limit.

To disable background threads (for example, during a bulk load):

```rust
let mut config = EnvironmentConfig::new(PathBuf::from("/data"));
config.set_allow_create(true);
config.set_run_cleaner(false);     // disable log cleaner during bulk load
config.set_run_checkpointer(false);
let env = Environment::open(config)?;
```

Note: disabling the cleaner means log files will accumulate on disk until the cleaner is re-enabled or the environment is reopened with the cleaner enabled.

### Sizing the Cache

The in-memory cache is the single biggest lever for Noxu DB performance. If the working set fits in the cache, reads require no disk I/O. If the working set exceeds the cache, performance degrades as records must be fetched from disk.

Configure cache size at environment open time:

```rust
let config = EnvironmentConfig::new(PathBuf::from("/data"))
    .with_allow_create(true)
    .with_cache_size(512 * 1024 * 1024);  // 512 MB
```

Guidelines for sizing:

- Start with an estimate of your hot working set (the records most frequently accessed).
- Monitor disk I/O after deploying. If the application reads from disk frequently even after warm-up, increase the cache.
- The cache starts at a small fraction of its configured maximum and grows as records are accessed. Full cache utilization is only reached after sufficient read/write activity.
- Leave headroom for your application's other memory usage (stack, heap, OS buffers). The cache is not locked into physical memory — it can be paged out by the OS under memory pressure.

A simple heuristic: if your database contains `N` records of average size `S` bytes, the minimum useful cache is roughly `N * S * 1.3` (the 1.3 factor accounts for B-tree internal node overhead).

### Log File Management

Noxu DB uses a write-once, append-only log. All database modifications are appended to the current log file. When a log file reaches its maximum size, a new one is created. Log files are named with 8-digit hexadecimal numbers: `00000000.ndb`, `00000001.ndb`, and so on.

Because records are updated in place in the B-tree cache but written as new log entries, old log entries become obsolete over time. The cleaner thread identifies log files where the proportion of live data has fallen below the utilization threshold and migrates the remaining live records to newer log files. Once a log file contains no live records it is deleted.

There is no separate "compact" or "vacuum" operation. The cleaner runs continuously in the background.

### Configuring Lock Timeouts

Long-running transactions or high lock contention can cause lock timeout errors. Adjust timeouts in `EnvironmentConfig`:

```rust
let config = EnvironmentConfig::new(PathBuf::from("/data"))
    .with_allow_create(true)
    .with_transactional(true);

// Use mutable setter for fields without builder methods
let mut config = config;
config.set_lock_timeout(2000);    // 2 seconds
config.set_txn_timeout(30_000);   // 30 seconds (0 = no limit)

let env = Environment::open(config)?;
```

---

## 12. Backup and Recovery

### How Noxu DB Stores Data

All Noxu DB data is stored in log files in the environment directory. There is no separate "data file" or "index file" — the log is the database. This simplifies backup considerably: to back up a Noxu DB database, you copy the log files.

Log files are write-once and immutable once closed. New writes go to the current (latest) log file. Older log files can be copied at any time without interrupting the running application.

### Normal Recovery

Every time an environment is opened, Noxu DB performs normal recovery. Normal recovery reads the log files since the last checkpoint, reconstructs the B-tree state, and rolls back any transactions that were in progress at the time of the previous shutdown (whether clean or crash). This process is automatic and transparent to the application.

Normal recovery is fast because checkpoints bound the amount of log that must be replayed. A clean shutdown writes a final checkpoint, so recovery from a clean shutdown is nearly instantaneous.

### Hot Backup

A hot backup captures the database while it is running and accepting writes. The process:

1. Copy all log files (`*.ndb`) from the environment directory to the backup location. Files must be copied in alphabetical order (which is also numerical order by file number).
2. While the backup is running, the environment may create new log files. A second pass is usually needed to capture those.
3. The backup is complete when no new log files are created between two successive directory listings.

A hot backup captures the database as of the most recently flushed checkpoint. Modifications that were only in the in-memory cache at the time of the backup may not be included. To guarantee that all modifications are on disk before starting the backup, trigger an environment sync first (if supported by your build).

```rust
// List all .ndb files in the environment directory, copy in order
let env_dir = std::path::Path::new("/var/data/myapp");
let backup_dir = std::path::Path::new("/backup/myapp");
std::fs::create_dir_all(backup_dir)?;

let mut log_files: Vec<_> = std::fs::read_dir(env_dir)?
    .filter_map(|e| e.ok())
    .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("ndb"))
    .collect();
log_files.sort_by_key(|e| e.file_name());

for entry in &log_files {
    let dest = backup_dir.join(entry.file_name());
    std::fs::copy(entry.path(), dest)?;
}
```

### Offline Backup

An offline backup guarantees that the backup contains all modifications, including those that may still be in the in-memory cache. The process:

1. Stop all write activity to the database.
2. Close the environment cleanly. A clean close writes all dirty cache pages to the log and writes a final checkpoint.
3. Copy all log files from the environment directory to the backup location.
4. Reopen the environment and resume operations.

Offline backup produces the most consistent snapshot but requires a write pause.

### Incremental Backup

Noxu DB log files, once written and sealed, are never modified. This makes incremental backups straightforward: back up only the log files that have been created or modified since the last backup.

A simple approach: after each backup, record the number (name) of the last log file copied. On the next backup, copy only log files with higher numbers.

```rust
// After backup, record the last file number in persistent storage
let last_backup_file = "00000042.ndb"; // load from your persistent state

// On next backup, copy files newer than last_backup_file
let mut new_files: Vec<_> = std::fs::read_dir(env_dir)?
    .filter_map(|e| e.ok())
    .filter(|e| {
        let name = e.file_name();
        let name_str = name.to_str().unwrap_or("");
        name_str.ends_with(".ndb") && name_str > last_backup_file
    })
    .collect();
new_files.sort_by_key(|e| e.file_name());

for entry in &new_files {
    let dest = backup_dir.join(entry.file_name());
    std::fs::copy(entry.path(), dest)?;
}
```

### Catastrophic Recovery

To restore from a backup after a catastrophic failure:

1. Create a fresh environment directory (or clear the existing one).
2. Copy the backed-up log files into the environment directory.
3. Open the environment. Noxu DB will perform normal recovery and reconstruct the B-tree from the log files.

```rust
// Copy backup files to fresh environment directory
let restore_dir = std::path::Path::new("/var/data/myapp-restored");
std::fs::create_dir_all(restore_dir)?;

for entry in std::fs::read_dir(backup_dir)? {
    let entry = entry?;
    if entry.path().extension().and_then(|s| s.to_str()) == Some("ndb") {
        std::fs::copy(entry.path(), restore_dir.join(entry.file_name()))?;
    }
}

// Open the restored environment
let env = Environment::open(
    EnvironmentConfig::new(restore_dir.to_path_buf())
        .with_allow_create(false)  // directory already exists
        .with_transactional(true)
)?;
// Normal recovery happens automatically during open
```

### Backup Best Practices

- Use offline backup before major schema migrations or bulk data loads.
- For production systems with continuous writes, use hot backup combined with monitoring to ensure backup coverage does not fall too far behind.
- Always verify a backup by opening the restored environment and performing a sanity check (e.g., record count comparison).
- Retain multiple generations of backups. Log files are immutable once written, so older backups remain valid as long as the log files they reference exist.
- After a successful backup, it is safe to allow the cleaner to delete cleaned log files. If you want to prevent the cleaner from deleting files that are being backed up, coordinate the backup window with the cleaner.

---

## Quick Reference

### Minimal Application Template

```rust
use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    Get, OperationStatus,
};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Open environment
    let env = Environment::open(
        EnvironmentConfig::new(PathBuf::from("/tmp/myapp"))
            .with_allow_create(true)
            .with_transactional(true),
    )?;

    // 2. Open database
    let db = env.open_database(
        None,
        "mydb",
        &DatabaseConfig::new().with_allow_create(true),
    )?;

    // 3. Write
    db.put(
        None,
        &DatabaseEntry::from_bytes(b"hello"),
        &DatabaseEntry::from_bytes(b"world"),
    )?;

    // 4. Read
    let mut data = DatabaseEntry::new();
    if db.get(None, &DatabaseEntry::from_bytes(b"hello"), &mut data)?
        == OperationStatus::Success
    {
        println!("{}", std::str::from_utf8(data.data())?);
    }

    // 5. Scan
    let mut cursor = db.open_cursor(None, None)?;
    let mut key = DatabaseEntry::new();
    let mut val = DatabaseEntry::new();
    let mut status = cursor.get(&mut key, &mut val, Get::First, None)?;
    while status == OperationStatus::Success {
        println!("{:?} => {:?}", key.data(), val.data());
        status = cursor.get(&mut key, &mut val, Get::Next, None)?;
    }
    cursor.close()?;

    // 6. Close (or rely on Drop)
    db.close()?;
    env.close()?;
    Ok(())
}
```

### Common Patterns Summary

| Task | API |
|---|---|
| Open environment | `Environment::open(EnvironmentConfig::new(path).with_allow_create(true))` |
| Open database | `env.open_database(None, "name", &DatabaseConfig::new().with_allow_create(true))` |
| Insert/update record | `db.put(None, &key, &data)` |
| Read by key | `db.get(None, &key, &mut data)` |
| Delete by key | `db.delete(None, &key)` |
| Count records | `db.count()` |
| Open cursor | `db.open_cursor(None, None)` |
| First record | `cursor.get(&mut key, &mut data, Get::First, None)` |
| Next record | `cursor.get(&mut key, &mut data, Get::Next, None)` |
| Search exact | `cursor.get(&mut key, &mut data, Get::Search, None)` |
| Search range | `cursor.get(&mut key, &mut data, Get::SearchGte, None)` |
| Delete at cursor | `cursor.delete()` |
| Begin transaction | `env.begin_transaction(None, None)` |
| Commit transaction | `txn.commit()` |
| Abort transaction | `txn.abort()` |
| List database names | `env.get_database_names()` |
| Remove database | `env.remove_database(None, "name")` |
| Rename database | `env.rename_database(None, "old", "new")` |

---

## Further Reading

- `examples/simple.rs` — basic put/get/delete/cursor operations
- `examples/cursor_scan.rs` — forward scan, reverse scan, search, cursor delete
- `examples/transactions.rs` — transactional put, commit, and abort
- `examples/binding.rs` — typed bindings: integers, strings, doubles
- `examples/secondary.rs` — secondary databases and secondary cursors
- `examples/getting_started.rs` — complete worked example
- `examples/collections.rs` — collections API over raw databases
- `examples/persist.rs` — persistence and schema evolution
- `docs/AUDIT_REPORT.md` — implementation audit and known gaps
- `docs/JE_FIDELITY_REVIEW.md` — JE API fidelity analysis
