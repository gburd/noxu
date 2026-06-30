// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! # Noxu DB — umbrella crate
//!
//! **`noxu`** is the single crate users depend on to get the full
//! Noxu DB engine.  It re-exports the public API of all component crates
//! behind a single name and version so you write only:
//!
//! ```toml
//! [dependencies]
//! noxu = "7"
//! ```
//!
//! ## Quick-start
//!
//! ```no_run
//! use noxu::{DatabaseConfig, Environment, EnvironmentConfig};
//! use std::path::PathBuf;
//!
//! # fn main() -> noxu::Result<()> {
//! let env = Environment::open(
//!     EnvironmentConfig::new(PathBuf::from("/tmp/mydb"))
//!         .with_allow_create(true)
//!         .with_transactional(true),
//! )?;
//! let db_config = DatabaseConfig::new()
//!     .with_allow_create(true)
//!     .with_transactional(true);
//! let db = env.open_database(None, "kv", &db_config)?;
//! let txn = env.begin_transaction(None)?;
//! db.put_in(&txn, b"hello", b"world")?;
//! txn.commit()?;
//!
//! // Reads return `Result<Option<Bytes>>`.
//! if let Some(value) = db.get(b"hello")? {
//!     assert_eq!(value.as_ref(), b"world");
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Feature flags
//!
//! | Feature | Default | What it enables |
//! |---|---|---|
//! | `collections` | yes | [`collections`] module — `StoredMap`, `StoredSet`, `StoredList` |
//! | `persist` | yes | [`persist`] module — `#[derive(Entity)]`, `PrimaryIndex`, `EntityStore` |
//! | `xa` | yes | [`xa`] module — XA two-phase-commit (`XaEnvironment`) |
//! | `replication` | no | `replication` module — master-replica HA, elections |
//! | `replication-tls-rustls` | no | TLS for replication via pure-Rust `rustls` |
//! | `replication-tls-native` | no | TLS for replication via OS/OpenSSL |
//! | `observability` | no | `observe` module — `tracing` + `metrics` glue |
//!
//! ## Using Noxu from async code
//!
//! **Noxu is synchronous by design.** Every operation (`get`, `put`,
//! `commit`, cursor navigation, `Environment::open`) is blocking: it does
//! real disk I/O, acquires locks, and may park the calling thread. There is
//! no `async` API and none is planned — the engine uses explicit threads and
//! blocking I/O throughout (only the optional `replication` feature's
//! networking uses `tokio` internally).
//!
//! If you call Noxu from inside a `tokio` (or other async) runtime, do **not**
//! call it directly on an async worker thread — a blocking call there stalls
//! every other task sharing that worker. Instead, move the work onto a
//! blocking thread:
//!
//! ```ignore
//! # async fn example(env: std::sync::Arc<noxu::Environment>) -> Result<(), Box<dyn std::error::Error>> {
//! // `env` is Send + Sync; clone the Arc into the blocking task.
//! let value = tokio::task::spawn_blocking(move || {
//!     let db_cfg = noxu::DatabaseConfig::new().with_allow_create(true);
//!     let db = env.open_database(None, "users", &db_cfg)?;
//!     db.put(b"k", b"v")?;
//!     db.get(b"k")
//! })
//! .await??; // first `?`: JoinError; second `?`: NoxuError
//! # let _ = value;
//! # Ok(())
//! # }
//! ```
//!
//! Guidelines:
//!
//! - Wrap each unit of Noxu work in `tokio::task::spawn_blocking` (or a
//!   dedicated blocking thread pool), not the async path.
//! - **Never hold a [`Transaction`] (or an open [`Cursor`]) across an
//!   `.await`.** A transaction holds locks; suspending the task while it is
//!   open can block other writers indefinitely and the borrow on the
//!   transaction would also prevent the future from being `Send`. Open,
//!   use, and commit/abort a transaction entirely within one
//!   `spawn_blocking` closure.
//! - `Environment` is `Send + Sync`, so share it across tasks via
//!   `Arc<Environment>` and open per-task databases/transactions inside the
//!   blocking closure.
//!
//! ## Derive macros
//!
//! With the `persist` feature (on by default) the derive macros
//! `Entity`, `PrimaryKey`, and `SecondaryKey` are available directly
//! through this crate:
//!
//! ```no_run
//! use noxu::persist::{Entity, SecondaryKey};
//!
//! #[derive(Clone, Entity, SecondaryKey)]
//! struct User {
//!     #[primary_key]
//!     id: u64,
//!     #[secondary_key(name = "by_email", relate = OneToOne)]
//!     email: String,
//! }
//! ```

// Re-export the entire core public API at the crate root.
pub use noxu_db::*;

/// Binding helpers: tuple encoding, entry views, serial encoding.
pub mod bind {
    pub use noxu_bind::*;
}

/// Iterator-based collection views (`StoredMap`, `StoredSet`, `StoredList`).
#[cfg(feature = "collections")]
pub mod collections {
    pub use noxu_collections::*;
}

/// Trait-based entity persistence (DPL): `Entity`, `PrimaryKey`,
/// `PrimaryIndex`, `EntityStore`, and the derive macros.
///
/// The derive macros (`#[derive(Entity)]`, `#[derive(PrimaryKey)]`,
/// `#[derive(SecondaryKey)]`) are re-exported here so that the generated
/// code — which references `::noxu::persist::…` paths — resolves
/// correctly when the user only depends on the `noxu` umbrella crate.
#[cfg(feature = "persist")]
pub mod persist {
    pub use noxu_persist::*;
    // Re-export the derive macros so `use noxu::persist::Entity;` brings
    // both the trait AND the derive macro into scope.
    pub use noxu_persist_derive::{Entity, PrimaryKey, SecondaryKey};
}

/// XA distributed transactions (X/Open XA two-phase commit).
#[cfg(feature = "xa")]
pub mod xa {
    pub use noxu_xa::*;
}

/// Master-replica high-availability replication.
#[cfg(feature = "replication")]
pub mod replication {
    pub use noxu_rep::*;
}

/// Optional observability integration (`tracing` + `metrics` + OpenTelemetry).
#[cfg(feature = "observability")]
pub mod observe {
    pub use noxu_observe::*;
}
