# Databases

## What is a Database?

A Noxu DB database is a named B-tree stored within an environment. Each database holds a collection of key/data records. You can think of it as a sorted map from byte-array keys to byte-array values.

Multiple databases can coexist in the same environment. They share the environment's cache and background threads but are otherwise independent B-trees.

On disk, all databases in an environment are stored together in the environment's log files — there are no separate per-database files.

## Opening a Database

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

## Database Configuration

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

## Multiple Databases in One Environment

```rust
let customers = env.open_database(None, "customers", &DatabaseConfig::new().with_allow_create(true))?;
let orders    = env.open_database(None, "orders",    &DatabaseConfig::new().with_allow_create(true))?;
let products  = env.open_database(None, "products",  &DatabaseConfig::new().with_allow_create(true))?;
```

All three databases share the environment's cache and can participate in the same transactions.

## Closing a Database

```rust
db.close()?;
```

After calling `close`, the handle can no longer be used. Any active cursors on the database are invalidated. Always close all cursors before closing the database.

## Checking Whether a Database Handle is Valid

```rust
if db.is_valid() {
    // safe to use
}
```

## Getting the Record Count

```rust
let count: u64 = db.count()?;
println!("{} records in database", count);
```

---

