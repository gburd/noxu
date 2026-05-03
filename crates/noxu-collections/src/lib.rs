#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Iterator-based collection views for Noxu DB.
//!
//! Port of `com.sleepycat.collections`  -  provides standard Rust Iterator
//! and collection-style access to databases.
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
//!
//! # Key Index
//!
//! The collection views maintain an internal key index (`BTreeSet`) to
//! support iteration. This index is populated automatically when records
//! are inserted through the collection views (e.g., `StoredMap::put()`).
//! For databases with pre-existing data, use `register_key()` or
//! `register_keys()` to populate the index.
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
