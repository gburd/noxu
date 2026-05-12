# Secondary Databases

## What is a Secondary Database?

A secondary database is an index over a primary database. While the primary database stores your canonical records keyed by a primary key (e.g., employee ID), a secondary database stores an additional mapping from some derived key (e.g., department name) to the primary key.

Secondary databases are read-only from your application's perspective — you do not insert into them directly. Instead, whenever you update the primary database, you update the secondary index to reflect the change.

## Implementing a Key Creator

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

## Opening a Secondary Database

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

## Reading from a Secondary Database

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

## Iterating a Secondary Database

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

## Keeping the Secondary Index in Sync

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

## Closing a Secondary Database

```rust
secondary.close()?;
```

The secondary must be closed before the primary database and before the environment.

---

