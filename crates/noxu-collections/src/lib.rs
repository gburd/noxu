#![forbid(unsafe_code)]
// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT
#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! > **Internal component of the [`noxu`](https://crates.io/crates/noxu) database.**
//! >
//! > This crate is published only so the `noxu` umbrella crate can depend on it.
//! > Use `noxu` (`noxu = "3"`) in applications; depend on this crate directly only
//! > if you are extending the engine internals. Its API may change without a major
//! > version bump.
//!
//! Iterator-based collection views for Noxu DB.
//!
//! Provides typed map / set / list / iterator views over Noxu DB
//! databases, allowing database records to be accessed through
//! familiar Rust collection patterns.
//!
//! Every `Stored*` type is parameterised by [`noxu_bind::EntryBinding`]
//! implementations for keys and/or values; the public methods are
//! generic over the typed `K` / `V` rather than over `&[u8]`.  Every
//! method accepts `txn: Option<&noxu_db::Transaction>` as the leading
//! argument — the same convention as `noxu_db::Database` /
//! `noxu_db::SecondaryDatabase`:
//!
//! - `None` runs the operation as auto-commit (the engine allocates
//!   a synthetic auto-txn for each call).
//! - `Some(&t)` participates in the caller's transaction.
//!
//! # Overview
//!
//! - [`StoredMap<K, V, KB, VB>`] — typed map view of a primary database.
//! - [`StoredSortedMap<K, V, KB, VB>`] — typed map plus sorted-map
//!   navigation (`first_key`, `last_key`, `iter_from`, `iter_reverse`).
//! - [`StoredKeySet<K, KB>`] — typed set view of database keys.
//! - [`StoredValueSet<V, VB>`] — typed collection view of database
//!   values.
//! - [`StoredList<V, VB>`] — typed indexed list with shift-down
//!   compaction on `remove`.
//! - [`StoredIterator<T>`] — generic snapshot iterator yielding
//!   typed items.
//! - [`TransactionRunner`] — runs a closure under a runner-managed
//!   transaction with jittered exponential backoff retry on
//!   deadlock / lock-conflict.  In v1.6 the `&Transaction` it
//!   supplies can be threaded straight into any Stored* method.
//!
//! # Example
//!
//! ```ignore
//! use noxu_bind::{IntBinding, StringBinding};
//! use noxu_collections::{StoredMap, TransactionRunner};
//! use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
//!
//! let env = Environment::open(env_config)?;
//! let db_config = DatabaseConfig::new().with_allow_create(true);
//! let db  = env.open_database(None, "users", &db_config)?;
//!
//! let map: StoredMap<i32, String, _, _> =
//!     StoredMap::new(&db, IntBinding, StringBinding);
//!
//! // Auto-commit:
//! map.put(None, &1, &"alice".to_string())?;
//!
//! // Participate in a runner-managed txn:
//! let runner = TransactionRunner::new(&env);
//! runner.run(|txn| {
//!     map.put(Some(txn), &2, &"bob".to_string())?;
//!     map.remove(Some(txn), &1)?;
//!     Ok(())
//! })?;
//! ```
//!
//! # Migration from v1.5
//!
//! The v1.5 → v1.6 transition breaks the v1.5 `&[u8]`-keyed surface.  See
//! `docs/src/getting-started/migrating.md` for the detailed
//! before/after.  In short:
//!
//! - `StoredMap::new(&db, false)` → `StoredMap::new(&db, key_binding,
//!   value_binding)`.
//! - `map.get(b"k")` → `map.get(None, &k)`.
//! - The internal `BTreeSet` key index, `register_key` /
//!   `register_keys` / `known_keys` accessors are removed.
//!   Iteration walks the database directly via a cursor.
//! - `StoredList::remove` now shifts every higher index down by one
//!   slot and decrements `next_index`.

pub mod error;
pub(crate) mod internal;
pub mod stored_iterator;
pub mod stored_key_set;
pub mod stored_list;
pub mod stored_map;
pub mod stored_sorted_map;
pub mod stored_value_set;
pub mod transaction_runner;

// Re-export commonly used types
pub use error::{CollectionError, Result};
pub use stored_iterator::StoredIterator;
pub use stored_key_set::StoredKeySet;
pub use stored_list::StoredList;
pub use stored_map::StoredMap;
pub use stored_sorted_map::StoredSortedMap;
pub use stored_value_set::StoredValueSet;
pub use transaction_runner::{RetryConfig, TransactionRunner};
