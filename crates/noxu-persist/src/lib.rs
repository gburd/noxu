#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Derive-macro-based entity persistence for Noxu DB.
//!
//! Direct Persistence Layer — provides
//! trait-based entity-to-database mapping. Users implement `Entity`,
//! `PrimaryKey`, and `EntitySerializer` traits for their types. Derive
//! macros can be added later in a separate proc-macro crate.
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

pub mod database_namer;
pub mod entity;
pub mod entity_serializer;
pub mod entity_store;
pub mod error;
pub mod evolve;
pub mod key_selector;
pub mod primary_index;
pub mod secondary_index;
pub mod sequence;
pub mod simple_serializer;
pub mod store_config;

// Re-export commonly used types
pub use database_namer::{
    CustomDatabaseNamer, DatabaseNamer, DefaultDatabaseNamer,
};
pub use entity::{Entity, PrimaryKey};
pub use entity_serializer::EntitySerializer;
pub use entity_store::EntityStore;
pub use error::{PersistError, Result};
pub use key_selector::{
    AllKeysSelector, KeySelector, NotKeySelector, PredicateKeySelector,
    RangeKeySelector, SetKeySelector,
};
pub use primary_index::{EntityIterator, KeyIterator, PrimaryIndex};
pub use secondary_index::SecondaryIndex;
pub use sequence::{MemorySequence, Sequence};
pub use simple_serializer::{FieldDecoder, FieldEncoder, SimpleSerializer};
pub use store_config::StoreConfig;

// Schema evolution re-exports
pub use evolve::{
    ClassMutations, ConversionFn, Converter, Deleter, EvolveConfig, EvolveListener, EvolveStats,
    Mutations, MutationKey, Renamer,
};
