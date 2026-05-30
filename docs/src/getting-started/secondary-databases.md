# Secondary Databases

> **v1.6 capability matrix:** see
> [Introduction → v1.6 capability matrix](../introduction.md#v15-capability-matrix).

## What is a Secondary Database?

A secondary database is an index over a primary database. While the primary
database stores your canonical records keyed by a primary key (e.g.
employee ID), a secondary database stores an additional mapping from some
derived key (e.g. department name) to the primary key.

Secondary databases are read-only from your application's perspective —
you do not insert into them directly. **As of v1.6 the primary database
maintains every registered secondary index automatically**: when you
`primary.put(...)` or `primary.delete(...)`, every `SecondaryDatabase`
opened against that primary is updated under the same caller-supplied
transaction, so the primary record and its index entries commit or abort
atomically. Manual `update_secondary` calls are still available as an
escape hatch for population from external feeds, but ordinary application
code no longer needs them.

## v1.6 contract

Three v1.5 limitations are closed in v1.6:

1. **Sorted-dup secondaries** (Decision 1B / audit C4). Multiple
   primary records may produce the same secondary key. The inner
   index storage stores them as duplicates of the secondary key; cursor
   walks via `SecondaryCursor::get_next_dup_full` /
   `get_prev_dup_full` enumerate every primary that shares the
   secondary key.

   The inner index database **must** be opened with
   `DatabaseConfig::with_sorted_duplicates(true)`. A non-sorted-dup
   inner DB causes `SecondaryDatabase::open` to return
   `NoxuError::IllegalArgument`.

2. **Automatic associate()-style maintenance** (audit C3). Every
   `SecondaryDatabase` registers itself on the primary at open time;
   `Database::put` and `Database::delete` walk the registry under the
   caller's txn so primary writes and secondary index updates commit
   or abort together. Update-existing-key semantics (delete-old +
   insert-new) and multi-key creators are honoured.

3. **Foreign-key constraints** (Decision 2C / audit C2). When the
   secondary's `SecondaryConfig::with_foreign_key_database_handle(...)`
   names a foreign primary, that foreign DB's `delete` triggers the
   configured `ForeignKeyDeleteAction`:

   * `Abort` — return `NoxuError::ForeignConstraintViolation` and
     leave the foreign record in place.
   * `Cascade` — delete every child primary record indexed under the
     foreign key, transitively, with cycle detection.
   * `Nullify` — call the user's `ForeignKeyNullifier` (single-key)
     or `ForeignMultiKeyNullifier` (multi-key) to mutate the child
     primary's data; auto-maintenance removes the now-stale secondary
     entry.

   Setting `foreign_key_database_name` (the legacy advisory setter)
   without the matching handle is rejected with
   `NoxuError::IllegalArgument` so callers do not silently end up with
   an unenforced constraint.

See `crates/noxu-db/tests/secondary_decisions_test.rs` for the regression
tests that exercise each behaviour.

## Implementing a Key Creator

A key creator extracts the secondary key from a primary record. Implement
the `SecondaryKeyCreator` trait:

```rust
use noxu::{Database, DatabaseEntry, SecondaryKeyCreator};

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

The method returns `true` if a secondary key was produced, or `false` if
this primary record should have no entry in the secondary database.

For records that produce more than one secondary key (e.g. tags), use the
`SecondaryMultiKeyCreator` trait instead.

## Opening a Secondary Database

```rust
use noxu::{SecondaryConfig, SecondaryDatabase};
use std::sync::Arc;
use parking_lot::Mutex;

// Open the primary database.
let primary_db = env.open_database(
    None, "employees",
    &DatabaseConfig::new().with_allow_create(true),
)?;
let primary = Arc::new(Mutex::new(primary_db));

// Open the underlying storage for the secondary index.  v1.6 sorted-dup
// secondaries require the inner DB to allow duplicates.
let sec_db = env.open_database(
    None, "by_department",
    &DatabaseConfig::new()
        .with_allow_create(true)
        .with_sorted_duplicates(true),
)?;

// Create and open the secondary database.
let sec_config = SecondaryConfig::new()
    .with_allow_create(true)
    .with_allow_populate(true)
    .with_key_creator(Box::new(DepartmentKeyCreator));

let secondary = SecondaryDatabase::open(
    Arc::clone(&primary), sec_db, sec_config,
)?;
```

## Reading from a Secondary Database

```rust
let dept_key = DatabaseEntry::from_bytes(b"Engineering");
let mut primary_key = DatabaseEntry::new();
let mut data = DatabaseEntry::new();

let status = secondary.get(None, &dept_key, &mut primary_key, &mut data)?;
if status == OperationStatus::Success {
    let emp_name = std::str::from_utf8(primary_key.get_data().unwrap())?;
    let record = std::str::from_utf8(data.get_data().unwrap())?;
    println!("{}: {}", emp_name, record);
}
```

`SecondaryDatabase::get` returns the **first** primary indexed under the
secondary key (the smallest primary key in cursor order). To enumerate
every primary indexed under the same secondary key, use the cursor.

## Walking duplicates of a secondary key

```rust
let mut cursor = secondary.open_cursor(None, None)?;
let mut sec_key = DatabaseEntry::new();
let mut pk = DatabaseEntry::new();
let mut data = DatabaseEntry::new();

// Position on the first record under "Engineering".
let mut status = cursor.get_search_key(
    &DatabaseEntry::from_bytes(b"Engineering"),
    &mut pk,
    &mut data,
)?;
while status == OperationStatus::Success {
    println!("emp = {:?}", pk.get_data());
    // Step to the next primary indexed under the SAME secondary key,
    // or NotFound when the run ends.
    status = cursor.get_next_dup_full(
        &mut sec_key,
        &mut pk,
        &mut data,
    )?;
}
cursor.close()?;
```

`get_next_dup_full` and the symmetric `get_prev_dup_full` walk every
duplicate of the cursor's current secondary key and return
`OperationStatus::NotFound` as soon as the cursor leaves the run.

## Foreign-key constraints

```rust
use noxu::secondary_config::ForeignKeyDeleteAction;

// Open the foreign primary (e.g. a "departments" lookup table).
let depts = Arc::new(Mutex::new(env.open_database(
    None, "departments",
    &DatabaseConfig::new().with_allow_create(true),
)?));

// Open an "employees-by-department" index that references it.
let sec_db = env.open_database(
    None, "emp_by_dept",
    &DatabaseConfig::new()
        .with_allow_create(true)
        .with_sorted_duplicates(true),
)?;
let cfg = SecondaryConfig::new()
    .with_allow_create(true)
    .with_key_creator(Box::new(DepartmentKeyCreator))
    .with_foreign_key_database_handle(Arc::clone(&depts))
    .with_foreign_key_delete_action(ForeignKeyDeleteAction::Cascade);

let _emp_idx = SecondaryDatabase::open(
    Arc::clone(&primary), sec_db, cfg,
)?;
```

Now `depts.lock().delete(...)` cascades into every employee whose
department matches, all under the caller's txn.

## Closing a Secondary Database

```rust
secondary.close()?;
```

The secondary must be closed before the primary database and before the
environment.

---
