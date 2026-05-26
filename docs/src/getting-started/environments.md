# Database Environments

## What is an Environment?

An environment is a directory on disk plus an in-memory handle that manages everything in that directory. Every application using Noxu DB must use an environment — it is not optional. The environment:

- Provides the in-memory cache shared by all databases opened through it.
- Runs background threads (cleaner, checkpointer, evictor).
- Manages lock and transaction state.
- Corresponds to a specific directory path on disk.

## Opening an Environment

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

## Environment Configuration

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

## Read-Only Environments

A read-only environment can be opened against an environment directory that is currently being written by another process. No write operations are permitted. Background threads do not run in a read-only environment.

```rust
let config = EnvironmentConfig::new(PathBuf::from("/var/data/myapp"))
    .with_read_only(true);
let env = Environment::open(config)?;
assert!(env.is_read_only());
```

## Closing an Environment

Always close the environment when you are finished. All open database handles must be closed first, and there must be no active transactions.

```rust
// Close databases first
db.close()?;

// Then close the environment
env.close()?;
```

If the environment handle goes out of scope without being explicitly closed, the `Drop` implementation performs a best-effort close. Relying on `Drop` is acceptable for simple applications but explicit close is recommended to propagate any errors.

## Listing Databases in an Environment

```rust
let names: Vec<String> = env.get_database_names()?;
for name in &names {
    println!("database: {}", name);
}
```

## Renaming and Removing Databases

```rust
// Rename (the database must not currently be open)
env.rename_database(None, "old_name", "new_name")?;

// Remove permanently
env.remove_database(None, "db_name")?;
```

---
