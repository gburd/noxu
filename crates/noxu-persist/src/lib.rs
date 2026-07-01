#![forbid(unsafe_code)]
// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT
#![allow(clippy::type_complexity, clippy::too_many_arguments)]
//! > **Internal component of the [`noxu`](https://crates.io/crates/noxu) database.**
//! >
//! > This crate is published only so the `noxu` umbrella crate can depend on it.
//! > Use `noxu` (`noxu = "7"`) in applications; depend on this crate directly only
//! > if you are extending the engine internals. Its API may change without a major
//! > version bump.
//!
//! > **Note on derive macros**: `#[derive(Entity)]`, `#[derive(PrimaryKey)]`, and
//! > `#[derive(SecondaryKey)]` emit `::noxu::persist::` paths in generated code by
//! > default, so the `noxu` umbrella crate must be present.  Users who depend on
//! > `noxu-persist` directly (without the umbrella) can add
//! > `#[entity(crate = "noxu_persist")]` to each struct to redirect generated
//! > code to `::noxu_persist::…` instead — see the
//! > [crate-path escape hatch](#crate-path-escape-hatch) section below.
//!
//! Derive-macro-based entity persistence for Noxu DB.
//!
//! Direct Persistence Layer — provides trait-based entity-to-database
//! mapping with a proc-macro derive shortcut.
//!
//! ## Umbrella usage (recommended)
//!
//! Most users depend on the `noxu` umbrella crate and use `noxu::persist`
//! as their import path.  The derive macros default to emitting
//! `::noxu::persist::…` paths, which works automatically:
//!
//! ```no_run
//! use noxu::persist::{Entity, SecondaryKey};
//!
//! #[derive(Clone, Debug, Entity, SecondaryKey)]
//! struct User {
//!     #[primary_key]
//!     id: u64,
//!     #[secondary_key(name = "by_email", relate = OneToOne)]
//!     email: String,
//! }
//! ```
//!
//! ## Crate-path escape hatch
//!
//! Users who add `noxu-persist` as a **direct** dependency (without the
//! `noxu` umbrella) can add `#[entity(crate = "noxu_persist")]` to each
//! annotated struct.  Generated code then uses `::noxu_persist::…` paths
//! instead, and the umbrella crate is not required.
//!
//! ```ignore
//! // Cargo.toml: noxu-persist = "3"  (no noxu umbrella)
//! use noxu_persist::{Entity, SecondaryKey};
//!
//! #[derive(Clone, Debug, Entity, SecondaryKey)]
//! #[entity(crate = "noxu_persist")]
//! struct Widget {
//!     #[primary_key]
//!     id: u64,
//!     #[secondary_key(name = "by_kind", relate = ManyToOne)]
//!     kind: String,
//! }
//! ```
//!
//! The same `#[entity(crate = "…")]` attribute is recognised by all three
//! derives: `Entity`, `PrimaryKey`, and `SecondaryKey`.  Any valid Rust
//! module path is accepted; a malformed path produces a descriptive
//! compile error.
//!
//! # Overview
//!
//! The persistence layer provides typed access to database records through:
//!
//! - **`Entity`** - Trait marking a type as storable
//! - **`PrimaryKey`** - Trait for primary key types
//! - **`EntitySerializer`** - Trait for custom serialization strategies
//! - **`PrimaryIndex`** - Typed CRUD operations on entities by primary key
//! - **`EntityStore`** - Manages databases for entity types
//! - **`StoreConfig`** - Configuration for entity stores
//!
//! # Example
//!
//! ```ignore
//! use noxu_persist::*;
//! use noxu_db::{Environment, EnvironmentConfig};
//!
//! // Define an entity
//! struct User { id: u64, name: String }
//!
//! impl Entity for User {
//!     type PrimaryKey = u64;
//!     fn primary_key(&self) -> &u64 { &self.id }
//!     fn entity_name() -> &'static str { "User" }
//! }
//!
//! // Define a serializer
//! struct UserSerializer;
//! impl EntitySerializer<User> for UserSerializer {
//!     fn serialize(&self, user: &User) -> error::Result<Vec<u8>> { /* ... */ }
//!     fn deserialize(&self, bytes: &[u8]) -> error::Result<User> { /* ... */ }
//! }
//!
//! // Use the store
//! let config = StoreConfig::new("my_store").with_allow_create(true);
//! // let mut store = EntityStore::open(&env, config)?;
//! // let index: PrimaryIndex<u64, User> = store.get_primary_index()?;
//! // index.put(&UserSerializer, &User { id: 1, name: "Alice".into() })?;
//! ```

pub mod entity;
pub mod entity_serializer;
pub mod entity_store;
pub mod error;
pub mod evolve;
pub mod primary_index;
pub mod secondary_index;
pub mod secondary_spec;
pub mod sequence;
pub mod simple_serializer;
pub mod store_config;

// Re-export commonly used types
pub use entity::{Entity, PrimaryKey};
pub use entity_serializer::EntitySerializer;
pub use entity_store::EntityStore;
pub use error::{PersistError, Result};
pub use primary_index::{EntityIterator, KeyIterator, PrimaryIndex};
pub use secondary_index::SecondaryIndex;
pub use secondary_spec::{DeleteAction, Relate, SecondarySpec};

// Derive-macro re-exports — see `noxu-persist-derive`.
// The user only needs `noxu_persist` in their `Cargo.toml`; the derive
// crate is pulled in transitively.  This mirrors the `serde` /
// `serde_derive` re-export pattern.
pub use noxu_persist_derive::{Entity, PrimaryKey, SecondaryKey};
pub use sequence::{MemorySequence, Sequence};
pub use simple_serializer::{FieldDecoder, FieldEncoder, SimpleSerializer};
pub use store_config::StoreConfig;

// Schema evolution re-exports
pub use evolve::{
    CatalogEntry, ClassCatalog, ClassMutations, ConversionFn, Converter,
    DecodedRecord, Deleter, EvolveConfig, EvolveListener, EvolveStats,
    MAX_CLASS_TAG_LEN, MutationKey, Mutations, Renamer, catalog_db_name,
};
