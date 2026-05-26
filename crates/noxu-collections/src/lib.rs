#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Iterator-based collection views for Noxu DB.
//!
//! Provides standard Rust Iterator and collection-style access to databases.
//!
//! This crate provides map, set, and iterator views over Noxu DB databases,
//! allowing database records to be accessed through familiar Rust collection
//! patterns.
//!
//! # Overview
//!
//! - [`StoredMap`]  -  A map-like view of a database (key-value pairs)
//! - [`StoredSortedMap`]  -  A sorted map view with range operations
//! - [`StoredKeySet`]  -  A set view of database keys
//! - [`StoredValueSet`]  -  A collection view of database values
//! - [`StoredIterator`]  -  Iterator over key-value pairs
//! - [`StoredKeyIterator`]  -  Iterator over keys only
//! - [`StoredValueIterator`]  -  Iterator over values only
//! - [`TransactionRunner`]  -  Transaction execution helper with retry
//!   *(see v1.5 limitations below)*
//!
//! # Key Index
//!
//! The collection views maintain an internal key index (`BTreeSet`) to
//! support iteration. This index is populated automatically when records
//! are inserted through the collection views (e.g., `StoredMap::put()`).
//! For databases with pre-existing data, use `register_key()` or
//! `register_keys()` to populate the index.
//!
//! # v1.5 limitations
//!
//! The collections surface in v1.5 is intentionally narrower than the
//! BDB-JE `com.sleepycat.collections` contract.  These constraints are
//! tracked by the May 2026 collections/bind API audit and are scheduled
//! for revisit in v1.6.
//!
//! 1. **`Stored*` operations are auto-commit only.**  Every `get` /
//!    `put` / `remove` / `iter` call on `StoredMap`, `StoredSortedMap`,
//!    `StoredList`, `StoredKeySet`, and `StoredValueSet` issues the
//!    underlying `Database` call with `txn = None`.  There is no way to
//!    thread an externally-begun [`noxu_db::Transaction`] into a
//!    collection method in v1.5.  If you need transactional semantics
//!    across several writes, use the raw `Database::put` / `delete` API
//!    with an explicit txn.  Threading
//!    `Option<&Transaction>` through every collection method is on the
//!    v1.6 roadmap (audit findings #1, #3, #4).
//!
//! 2. **[`TransactionRunner`] cannot drive `Stored*` calls.**  The
//!    runner returns a `&Transaction` that no `Stored*` method accepts
//!    in v1.5.  It is still useful for sequencing raw
//!    `Database`/`Cursor` calls with deadlock retry, but treat its
//!    "runs Stored* operations in a transaction" rustdoc as aspirational
//!    (planned for v1.6).
//!
//! 3. **[`StoredList::new`] does not recover the next-index counter.**
//!    Use [`StoredList::open`] when reopening an existing list — `new`
//!    starts at index 0 and will overwrite existing records on
//!    subsequent pushes.  See the [`StoredList`] docs for details.
//!
//! 4. **[`StoredList::remove`] does not compact.**  It is a single-key
//!    delete; it leaves a hole at the removed index and does not
//!    re-number higher indices.  See the [`StoredList::remove`] docs.
//!
//! See `docs/src/collections/` for the user-facing v1.5 limitations
//! summary and `docs/src/internal/sprint-3-collections-restriction.md`
//! for the full audit-finding bookkeeping.
//!
//! # Example
//!
//! ```ignore
//! use noxu_db::{Environment, EnvironmentConfig, DatabaseConfig};
//! use noxu_collections::StoredMap;
//!
//! let env = Environment::open(config).unwrap();
//! let db = env.open_database(None, "mydb", &db_config).unwrap();
//!
//! let map = StoredMap::new(&db, false);
//! map.put(b"key1", b"value1").unwrap();
//! map.put(b"key2", b"value2").unwrap();
//!
//! for entry in map.iter().unwrap() {
//!     let (key, value) = entry.unwrap();
//!     println!("{:?} -> {:?}", key, value);
//! }
//! ```

pub mod error;
pub mod stored_iterator;
pub mod stored_key_set;
pub mod stored_list;
pub mod stored_map;
pub mod stored_sorted_map;
pub mod stored_value_set;
pub mod transaction_runner;

// Re-export commonly used types
pub use error::{CollectionError, Result};
pub use stored_iterator::{
    StoredIterator, StoredKeyIterator, StoredValueIterator,
};
pub use stored_key_set::StoredKeySet;
pub use stored_list::StoredList;
pub use stored_map::StoredMap;
pub use stored_sorted_map::StoredSortedMap;
pub use stored_value_set::StoredValueSet;
pub use transaction_runner::TransactionRunner;
