# Installation

Add Noxu DB to your `Cargo.toml`:

```toml
[dependencies]
noxu-db = "0.1"
```

Noxu DB has no runtime dependencies beyond the Rust standard library.
There is no server to start — it is an embedded in-process library.

## Minimum Supported Rust Version (MSRV)

Noxu DB requires Rust 1.85 or later.

## Feature Flags

| Feature | Default | Description |
|---------|---------|-------------|
| (none required) | — | Core embedded DB is always available |

---

## Conceptual Overview


## What is Noxu DB?

Noxu DB is an embedded, transactional key-value store. "Embedded" means it runs inside your application process — there is no separate server to start or manage. "Transactional" means it provides full ACID guarantees: Atomicity, Consistency, Isolation, and Durability.

Key characteristics:

- All data is stored as raw byte arrays (`&[u8]`). Any Rust type that can be serialized to bytes can be stored.
- Records consist of a key/data pair. Keys are used to look up data. Both keys and data are represented by `DatabaseEntry` objects.
- The B-tree is always sorted by key, so range scans are efficient.
- One or more databases live inside a single *environment*. The environment manages the shared cache, background threads, and the on-disk log files.
- Transactions are optional but recommended for any application that writes data.

## Architecture in Brief

A Noxu DB application has three layers:

```
Environment
  └── Database (named, multiple per environment)
        └── Records (key/data pairs in a B-tree)
```

All data is stored in sequentially numbered log files (`.ndb` extension) in the environment directory. There is no separate "database file" distinct from the log — the log is the database. When the environment is opened, Noxu DB performs normal recovery to bring the B-tree back to a consistent state from the log.

## Adding Noxu DB to a Project

Add the following to your `Cargo.toml`:

```toml
[dependencies]
noxu-db = { path = "crates/noxu-db" }

# Optional: typed bindings for integers, floats, strings
noxu-bind = { path = "crates/noxu-bind" }
```

---

