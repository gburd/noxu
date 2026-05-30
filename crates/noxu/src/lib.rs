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
//! noxu = "3"
//! ```
//!
//! ## Quick-start
//!
//! ```ignore
//! use noxu::{Environment, EnvironmentConfig};
//!
//! let env = Environment::open(
//!     EnvironmentConfig::new("/tmp/mydb".into()).with_allow_create(true)
//! )?;
//! let db = env.open_database(None, "kv", true)?;
//! let txn = env.begin_transaction(None)?;
//! db.put(&txn, b"hello", b"world")?;
//! txn.commit()?;
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
//! ## Derive macros
//!
//! With the `persist` feature (on by default) the derive macros
//! `Entity`, `PrimaryKey`, and `SecondaryKey` are available directly
//! through this crate:
//!
//! ```ignore
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
